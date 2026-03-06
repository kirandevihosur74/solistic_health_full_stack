"""Version list and get."""

import html as html_mod
import re
import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session as DBSession

from database import get_db, Version
from schemas import VersionOut, VersionDetail

logger = logging.getLogger(__name__)
router = APIRouter()


def _html_to_preview(html: str, max_len: int = 120) -> str:
    """Extract visible text for preview, skipping style/script and other non-content."""
    if not html:
        return ""
    # Remove style and script blocks so we don't show CSS/JS
    text = re.sub(r"<style[^>]*>[\s\S]*?</style>", " ", html, flags=re.IGNORECASE)
    text = re.sub(r"<script[^>]*>[\s\S]*?</script>", " ", text, flags=re.IGNORECASE)
    # Strip all remaining HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_mod.unescape(text).replace("&nbsp;", " ").replace("\xa0", " ")
    text = " ".join(text.split()).strip()
    return (text[:max_len] + "…") if len(text) > max_len else text


@router.get("/versions")
def list_versions(session_id: str, db: DBSession = Depends(get_db)):
    logger.info("[versions:list] session_id=%s", session_id)
    versions = (
        db.query(Version)
        .filter(Version.session_id == session_id)
        .order_by(Version.created_at.desc())
        .all()
    )
    logger.info("[versions:list] Returning %d versions", len(versions))
    return {
        "versions": [
            VersionOut(
                id=v.id,
                created_at=v.created_at.isoformat() if v.created_at else "",
                html_preview=_html_to_preview(v.html or ""),
                revision_number=v.revision_number or 0,
                content_type=v.content_type or "email",
            )
            for v in versions
        ]
    }


@router.delete("/versions")
def clear_versions(session_id: str, db: DBSession = Depends(get_db)):
    """Clear all versions for a session (resets preview)."""
    logger.info("[versions:clear] Clearing versions for session_id=%s", session_id)
    count = db.query(Version).filter(Version.session_id == session_id).delete()
    db.commit()
    logger.info("[versions:clear] Deleted %d versions", count)
    return {"deleted": count}


@router.get("/versions/{version_id}", response_model=VersionDetail)
def get_version(version_id: str, db: DBSession = Depends(get_db)):
    logger.info("[versions:get] Loading version_id=%s", version_id)
    v = db.query(Version).filter(Version.id == version_id).first()
    if not v:
        logger.warning("[versions:get] Version not found: %s", version_id)
        raise HTTPException(404, "Version not found")
    logger.info("[versions:get] Found rev=%d, html_size=%d chars", v.revision_number or 0, len(v.html or ""))
    return VersionDetail(
        id=v.id,
        created_at=v.created_at.isoformat() if v.created_at else "",
        html=v.html,
        revision_number=v.revision_number or 0,
    )
