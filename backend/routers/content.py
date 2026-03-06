"""Content generation, edit, compliance, validate-html, export."""

import hashlib
import io
import json
import re
import time
import zipfile
import logging

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import func
from sqlalchemy.orm import Session as DBSession

from database import get_db, Session, Message, Claim, Version, ApprovedAsset, new_uuid, utcnow
from schemas import (
    GenerateReq, GenerateResp, EditReq, EditResp,
    ComplianceReviewResp, ReviewItem, ValidateHtmlReq,
)
from services import (
    parse_html_compliance,
    validate_claims_exact,
    validate_assets,
    validate_img_sources,
    validate_no_invented_clinical,
    inject_claims_and_assets,
    sanitize_edit_html,
)
import llm

logger = logging.getLogger(__name__)
router = APIRouter()


def _run_compliance_review(body: GenerateReq, db: DBSession) -> ComplianceReviewResp:
    """Run full compliance review for a session."""
    sess = db.query(Session).filter(Session.id == body.session_id).first()
    if not sess:
        raise HTTPException(404, "Session not found")

    claims = db.query(Claim).filter(Claim.id.in_(body.claim_ids)).all()
    all_library_claims = db.query(Claim).all()
    logger.info("[compliance] Selected %d claims, library has %d total", len(claims), len(all_library_claims))

    latest_version = (
        db.query(Version)
        .filter(Version.session_id == body.session_id)
        .order_by(Version.created_at.desc())
        .first()
    )
    html_content = latest_version.html if latest_version else ""
    logger.info(
        "[compliance] Latest version: %s, html_size=%d chars",
        latest_version.id if latest_version else "None", len(html_content),
    )

    items: list[ReviewItem] = []

    if re.search(r"\{\{?CLAIM:[^}]+\}\}?", html_content):
        items.append(ReviewItem(
            check="Claim Placeholders Resolved",
            status="fail",
            detail="Found unresolved {CLAIM:...} placeholders in HTML. Claims must be rendered verbatim with data-claim-id.",
        ))

    html_claim_ids, html_claim_texts, html_asset_ids = parse_html_compliance(html_content)
    approved_claim_map = {c.claim_id or c.id: c for c in db.query(Claim).all()}
    approved_claim_map.update({c.id: c for c in db.query(Claim).all()})
    approved_asset_ids = {a.asset_id for a in db.query(ApprovedAsset).all()}

    claim_check = validate_claims_exact(html_claim_ids, html_claim_texts, approved_claim_map)
    items.append(claim_check)

    invented_check = validate_no_invented_clinical(html_content)
    items.append(invented_check)

    asset_check = validate_assets(html_asset_ids, approved_asset_ids)
    items.append(asset_check)

    img_check = validate_img_sources(html_content)
    if img_check:
        items.append(img_check)

    if claims and not html_claim_ids:
        items.append(ReviewItem(
            check="Claims Rendered",
            status="fail",
            detail="Claims were selected but none were rendered with data-claim-id in the HTML.",
        ))

    allowed_sources = ("clinical_literature", "prior_approved", "prescribing_info")
    all_traceable = all(c.citation and c.source in allowed_sources for c in claims)
    if all_traceable and claims:
        items.append(ReviewItem(
            check="Source Traceability",
            status="pass",
            detail="All claims have valid citations and traceable sources."
        ))
    else:
        items.append(ReviewItem(
            check="Source Traceability",
            status="fail",
            detail="Some claims lack proper citations or have unrecognized sources."
        ))

    categories = {c.category for c in claims}
    has_efficacy = "efficacy" in categories
    has_safety = "safety" in categories

    if has_efficacy and has_safety:
        items.append(ReviewItem(
            check="FDA Fair Balance",
            status="pass",
            detail="Efficacy and safety claims are both present, satisfying fair balance."
        ))
    elif has_efficacy and not has_safety:
        items.append(ReviewItem(
            check="FDA Fair Balance",
            status="fail",
            detail="Efficacy claims present without safety information. FDA requires fair balance."
        ))
    elif not has_efficacy:
        items.append(ReviewItem(
            check="FDA Fair Balance",
            status="pass",
            detail="No efficacy claims present; fair balance requirement does not apply."
        ))

    if html_content:
        has_isi = bool(re.search(r"(?i)(important safety information|safety information)", html_content))
        has_pi_ref = bool(re.search(r"(?i)(prescribing information|boxed warning)", html_content))
        has_hcp = bool(re.search(r"(?i)(healthcare professional|hcp)", html_content))

        if has_isi:
            items.append(ReviewItem(check="ISI Section Present", status="pass",
                detail="Important Safety Information section found in content."))
        else:
            items.append(ReviewItem(check="ISI Section Present", status="fail",
                detail="Missing Important Safety Information (ISI) section."))

        if has_pi_ref:
            items.append(ReviewItem(check="PI Reference", status="pass",
                detail="Reference to full Prescribing Information found."))
        else:
            items.append(ReviewItem(check="PI Reference", status="warn",
                detail="Consider adding a reference to the full Prescribing Information."))

        if has_hcp:
            items.append(ReviewItem(check="HCP Designation", status="pass",
                detail="'For healthcare professionals' designation found."))
        else:
            items.append(ReviewItem(check="HCP Designation", status="warn",
                detail="Consider adding 'For US healthcare professionals only' designation."))
    else:
        items.append(ReviewItem(check="Content Generated", status="warn",
            detail="No generated content found yet. Generate content before running full review."))

    if any(c.category == "indication" for c in claims):
        items.append(ReviewItem(check="Indication Statement", status="pass",
            detail="Approved indication statement is included."))
    else:
        items.append(ReviewItem(check="Indication Statement", status="warn",
            detail="Consider including the approved indication statement for completeness."))

    if html_content and re.search(r"(?i)(references|citations)", html_content):
        items.append(ReviewItem(check="References Section", status="pass",
            detail="References/citations section found in content."))
    elif html_content:
        items.append(ReviewItem(check="References Section", status="fail",
            detail="No references section found. All claims must cite their source."))

    channel = sess.content_type
    if html_content:
        html_size = len(html_content.encode("utf-8"))
        if channel == "email" and html_size > 102400:
            items.append(ReviewItem(check="Channel Compatibility", status="warn",
                detail=f"Email HTML is {html_size//1024}KB. Some email clients limit to 100KB."))
        elif channel == "banner":
            if "728" in html_content and "90" in html_content:
                items.append(ReviewItem(check="Channel Compatibility", status="pass",
                    detail="Banner dimensions (728×90) detected in content."))
            else:
                items.append(ReviewItem(check="Channel Compatibility", status="warn",
                    detail="Banner format selected but standard dimensions not confirmed in HTML."))
        else:
            items.append(ReviewItem(check="Channel Compatibility", status="pass",
                detail=f"Content is compatible with {channel} format."))

    non_approved = [c for c in claims if c.compliance_status != "approved"]
    if non_approved:
        items.append(ReviewItem(check="Claim Approval Status", status="fail",
            detail=f"{len(non_approved)} claim(s) have non-approved status. Only approved claims can be used."))
    elif claims:
        items.append(ReviewItem(check="Claim Approval Status", status="pass",
            detail="All selected claims have 'approved' compliance status."))

    if html_content and re.search(r"(?i)(all rights reserved|trademark)", html_content):
        items.append(ReviewItem(check="Legal Footer", status="pass",
            detail="Legal/trademark footer found."))
    elif html_content:
        items.append(ReviewItem(check="Legal Footer", status="warn",
            detail="Consider adding trademark and copyright footer."))

    statuses = [it.status for it in items]
    if "fail" in statuses:
        overall = "fail"
    elif "warn" in statuses:
        overall = "warn"
    else:
        overall = "pass"

    can_export = "fail" not in statuses
    pass_count = statuses.count("pass")
    warn_count = statuses.count("warn")
    fail_count = statuses.count("fail")
    logger.info(
        "[compliance] Result: overall=%s, can_export=%s — %d pass, %d warn, %d fail",
        overall, can_export, pass_count, warn_count, fail_count,
    )
    for it in items:
        if it.status != "pass":
            logger.info("[compliance]   %s %s: %s", it.status.upper(), it.check, it.detail)

    return ComplianceReviewResp(
        overall=overall,
        can_export=can_export,
        items=items,
    )


@router.post("/generate", response_model=GenerateResp)
def generate(body: GenerateReq, db: DBSession = Depends(get_db)):
    logger.info(
        "[generate] session_id=%s, claim_ids=%d selected",
        body.session_id, len(body.claim_ids),
    )
    sess = db.query(Session).filter(Session.id == body.session_id).first()
    if not sess:
        logger.warning("[generate] Session not found: %s", body.session_id)
        raise HTTPException(404, "Session not found")

    claims = db.query(Claim).filter(Claim.id.in_(body.claim_ids)).all()
    if not claims:
        logger.warning("[generate] No valid claims found for ids: %s", body.claim_ids)
        raise HTTPException(400, "No valid claims selected")

    claim_categories = [c.category for c in claims]
    logger.info("[generate] Matched %d claims: categories=%s", len(claims), claim_categories)

    max_rev = (
        db.query(func.max(Version.revision_number))
        .filter(Version.session_id == body.session_id)
        .scalar()
    ) or 0
    revision = max_rev + 1
    logger.info("[generate] This will be revision #%d", revision)

    messages = (
        db.query(Message)
        .filter(Message.session_id == body.session_id)
        .order_by(Message.created_at)
        .all()
    )
    conversation_context = "\n".join(f"{m.role}: {m.content}" for m in messages[-10:])
    logger.info("[generate] Conversation context: %d messages (last 10 of %d)", min(10, len(messages)), len(messages))

    session_context = {
        "content_type": sess.content_type,
        "audience": sess.audience or "hcp",
        "campaign_goal": sess.campaign_goal or "awareness",
        "tone": sess.tone or "clinical",
    }

    selected_asset_ids = body.selected_asset_ids or []
    if len(selected_asset_ids) > 3:
        selected_asset_ids = selected_asset_ids[:3]
    asset_ids_valid = []
    if selected_asset_ids:
        approved = db.query(ApprovedAsset).filter(ApprovedAsset.asset_id.in_(selected_asset_ids)).all()
        asset_ids_valid = [a.asset_id for a in approved]
        logger.info("[generate] Selected assets: %s (valid: %s)", selected_asset_ids, asset_ids_valid)

    claims_dicts = [
        {
            "claim_id": c.claim_id or c.id,
            "text": c.verbatim_text or c.text,
            "citation": c.citation,
            "category": c.category,
            "source": c.source,
        }
        for c in claims
    ]

    t0 = time.perf_counter()
    generated_html = llm.generate_content(claims_dicts, session_context, conversation_context)
    gen_ms = (time.perf_counter() - t0) * 1000
    logger.info("[generate] LLM HTML generated in %.1fms: %d chars", gen_ms, len(generated_html))
    generated_html = inject_claims_and_assets(generated_html, claims, asset_ids_valid)

    version = Version(
        id=new_uuid(),
        session_id=body.session_id,
        html=generated_html,
        content_type=sess.content_type,
        revision_number=revision,
        claim_ids_used=json.dumps(body.claim_ids),
        asset_ids_used=json.dumps(asset_ids_valid),
        created_at=utcnow(),
    )
    db.add(version)
    db.commit()
    logger.info("[generate] Saved version id=%s, rev=%d, html_size=%d", version.id, revision, len(generated_html))

    return GenerateResp(html=generated_html, revision_number=revision)


@router.post("/edit", response_model=EditResp)
def edit(body: EditReq, db: DBSession = Depends(get_db)):
    logger.info(
        "[edit] session_id=%s, instruction='%s', html_size=%d chars",
        body.session_id, body.instruction[:80], len(body.current_html),
    )
    sess = db.query(Session).filter(Session.id == body.session_id).first()
    if not sess:
        logger.warning("[edit] Session not found: %s", body.session_id)
        raise HTTPException(404, "Session not found")

    t0 = time.perf_counter()
    edited_html = llm.edit_content(body.current_html, body.instruction)
    edit_ms = (time.perf_counter() - t0) * 1000
    logger.info("[edit] LLM edit applied in %.1fms: %d -> %d chars (delta %+d)",
                 edit_ms, len(body.current_html), len(edited_html), len(edited_html) - len(body.current_html))

    latest = (
        db.query(Version)
        .filter(Version.session_id == body.session_id)
        .order_by(Version.created_at.desc())
        .first()
    )
    if latest:
        claim_ids_used = json.loads(latest.claim_ids_used or "[]")
        asset_ids_used = json.loads(latest.asset_ids_used or "[]")
        claims_for_edit = db.query(Claim).filter(Claim.id.in_(claim_ids_used)).all()
        edited_html = sanitize_edit_html(edited_html, claims_for_edit, asset_ids_used)
        logger.info("[edit] Sanitized: re-rendered %d claims, %d assets", len(claims_for_edit), len(asset_ids_used))

    max_rev = (
        db.query(func.max(Version.revision_number))
        .filter(Version.session_id == body.session_id)
        .scalar()
    ) or 0
    revision = max_rev + 1

    claim_ids_json = latest.claim_ids_used if latest else "[]"
    asset_ids_json = latest.asset_ids_used if latest else "[]"
    version = Version(
        id=new_uuid(),
        session_id=body.session_id,
        html=edited_html,
        content_type=sess.content_type,
        revision_number=revision,
        claim_ids_used=claim_ids_json,
        asset_ids_used=asset_ids_json,
        created_at=utcnow(),
    )
    db.add(version)
    db.commit()
    logger.info(
        "[edit] Saved version id=%s, rev=%d — carried forward claim_ids, asset_ids",
        version.id, revision,
    )

    return EditResp(html=edited_html, revision_number=revision)


@router.post("/compliance-review", response_model=ComplianceReviewResp)
def compliance_review(body: GenerateReq, db: DBSession = Depends(get_db)):
    """Comprehensive compliance review with green/yellow/red status per check."""
    logger.info(
        "[compliance] Running review — session_id=%s, claim_ids=%d",
        body.session_id, len(body.claim_ids),
    )
    return _run_compliance_review(body, db)


@router.post("/compliance-check")
def compliance_check(body: GenerateReq, db: DBSession = Depends(get_db)):
    result = _run_compliance_review(body, db)
    issues = [it.detail for it in result.items if it.status == "fail"]
    warnings = [it.detail for it in result.items if it.status == "warn"]
    return {"passed": result.overall != "fail", "issues": issues, "warnings": warnings}


@router.post("/validate-html")
def validate_html(body: ValidateHtmlReq, db: DBSession = Depends(get_db)):
    """Validate arbitrary HTML for compliance (for testing tamper scenarios)."""
    if re.search(r"\{\{?CLAIM:[^}]+\}\}?", body.html):
        return {
            "checks": [{
                "id": "claim_placeholders",
                "label": "Claims must be rendered (no placeholders)",
                "status": "fail",
                "details": "Found unresolved {CLAIM:...} placeholders in HTML.",
            }],
            "can_export": False,
        }
    html_claim_ids, html_claim_texts, html_asset_ids = parse_html_compliance(body.html)
    approved_claim_map = {c.claim_id or c.id: c for c in db.query(Claim).all()}
    approved_claim_map.update({c.id: c for c in db.query(Claim).all()})
    approved_asset_ids = {a.asset_id for a in db.query(ApprovedAsset).all()}

    checks = []
    claim_check = validate_claims_exact(html_claim_ids, html_claim_texts, approved_claim_map)
    checks.append({"id": "claim_exact_match", "label": claim_check.check, "status": claim_check.status, "details": claim_check.detail})
    invented_check = validate_no_invented_clinical(body.html)
    checks.append({"id": "no_invented_clinical", "label": invented_check.check, "status": invented_check.status, "details": invented_check.detail})
    asset_check = validate_assets(html_asset_ids, approved_asset_ids)
    checks.append({"id": "visual_assets", "label": asset_check.check, "status": asset_check.status, "details": asset_check.detail})
    img_check = validate_img_sources(body.html)
    if img_check:
        checks.append({"id": "image_sources", "label": img_check.check, "status": img_check.status, "details": img_check.detail})
    has_isi = bool(re.search(r"(?i)(important safety information|safety information)", body.html))
    checks.append({
        "id": "isi_section",
        "label": "ISI Section Present",
        "status": "pass" if has_isi else "fail",
        "details": "ISI found" if has_isi else "Missing ISI",
    })
    can_export = all(c["status"] != "fail" for c in checks)
    return {"checks": checks, "can_export": can_export}


@router.post("/export")
def export_content(body: GenerateReq, db: DBSession = Depends(get_db)):
    """Export final content package as zip."""
    logger.info("[export] Starting export — session_id=%s, claim_ids=%d", body.session_id, len(body.claim_ids))
    sess = db.query(Session).filter(Session.id == body.session_id).first()
    if not sess:
        logger.warning("[export] Session not found: %s", body.session_id)
        raise HTTPException(404, "Session not found")

    latest_version = (
        db.query(Version)
        .filter(Version.session_id == body.session_id)
        .order_by(Version.created_at.desc())
        .first()
    )
    if not latest_version:
        logger.warning("[export] No versions exist for session %s", body.session_id)
        raise HTTPException(400, "No content generated yet")

    logger.info("[export] Exporting version id=%s, rev=%d", latest_version.id, latest_version.revision_number)

    claims = db.query(Claim).filter(Claim.id.in_(body.claim_ids)).all()

    review = _run_compliance_review(body, db)
    if not review.can_export:
        logger.warning("[export] BLOCKED — compliance review has failures, cannot export")
        raise HTTPException(
            400,
            "Cannot export: compliance review has blocking failures. "
            "Resolve all red items first."
        )

    export_html = latest_version.html
    if re.search(r"\{\{?CLAIM:[^}]+\}\}?", export_html):
        raise HTTPException(400, "Cannot export: unresolved claim placeholders remain in HTML.")
    img_check = validate_img_sources(export_html)
    if img_check:
        raise HTTPException(400, f"Cannot export: {img_check.detail}")

    _, _, html_asset_ids = parse_html_compliance(export_html)
    html_asset_ids = list(dict.fromkeys(html_asset_ids))

    assets = []
    for aid in html_asset_ids:
        a = db.query(ApprovedAsset).filter(ApprovedAsset.asset_id == aid).first()
        if not a:
            raise HTTPException(400, f"Cannot export: HTML contains unknown asset ID: {aid}")
        assets.append(a)

    if asset_ids_from_version := json.loads(latest_version.asset_ids_used or "[]"):
        if not html_asset_ids:
            logger.warning(
                "[export] Selected assets not rendered — version had %s but HTML has no data-asset-id (template lacked {{ASSETS}} slot)",
                asset_ids_from_version,
            )

    def _sha256(s: str) -> str:
        return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

    claims_meta = [
        {
            "claim_id": c.claim_id or c.id,
            "verbatim_text": c.verbatim_text or c.text,
            "source_doc": c.source_doc or "prior_approved",
            "citation": c.citation,
            "sha256": c.text_sha256 or _sha256(c.verbatim_text or c.text or ""),
        }
        for c in claims
    ]
    assets_meta = [
        {
            "asset_id": a.asset_id,
            "filename": a.filename,
            "sha256": a.sha256,
            "source_doc": a.source_doc,
            "source_page": a.source_page,
        }
        for a in assets
    ]
    compliance_report = {
        "overall": review.overall,
        "can_export": review.can_export,
        "reviewed_at": utcnow().isoformat(),
        "checks": [
            {"id": it.check.lower().replace(" ", "_"), "label": it.check, "status": it.status, "details": it.detail}
            for it in review.items
        ],
    }

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("html/index.html", export_html)
        zf.writestr("metadata/claims.json", json.dumps(claims_meta, indent=2))
        zf.writestr("metadata/assets.json", json.dumps(assets_meta, indent=2))
        zf.writestr("compliance/report.json", json.dumps(compliance_report, indent=2))
        csv_rows = ["asset_id,filename,sha256,source_doc,source_page"]
        for a in assets:
            csv_rows.append(f"{a.asset_id},{a.filename},{a.sha256},{a.source_doc or ''},{a.source_page or ''}")
        zf.writestr("manifests/asset_manifest.csv", "\n".join(csv_rows))

    buf.seek(0)
    rev = latest_version.revision_number or 1
    logger.info("[export] Zip assembled — html, claims, assets, compliance, manifest")
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=fruzaqla-export-rev{rev}.zip"},
    )
