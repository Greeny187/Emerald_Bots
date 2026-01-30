import httpx
import io
import base64
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)

API_BASE = "https://api.x.com/2"

class XClientError(Exception):
    pass

async def _upload_media(access_token: str, media_bytes: bytes, media_type: str = "image/jpeg") -> str:
    """Upload media to X and return media_id."""
    if not media_bytes or len(media_bytes) == 0:
        raise XClientError("Media bytes erforderlich")

    # X API v1.1 for media upload
    url = "https://upload.twitter.com/1.1/media/upload.json"
    headers = {"Authorization": f"Bearer {access_token}"}

    try:
        logger.debug(f"X upload_media: bytes={len(media_bytes)} type={media_type}")
        async with httpx.AsyncClient(timeout=60) as client:
            files = {"media_data": media_bytes}
            r = await client.post(url, files=files, headers=headers)

            if r.status_code >= 400:
                logger.error(f"X media upload failed: {r.status_code} - {r.text}")
                raise XClientError(f"X media upload error {r.status_code}: {r.text[:200]}")

            data = r.json()
            media_id = data.get("media_id_string")
            if not media_id:
                raise XClientError("No media_id returned from X")

            logger.info(f"X media uploaded successfully: {media_id}")
            return media_id
    except httpx.RequestError as e:
        logger.exception(f"X media upload request error: {e}")
        raise XClientError(f"X media upload request failed: {str(e)}")

async def post_text(access_token: str, text: str, media_ids: Optional[List[str]] = None) -> dict:
    """Post text-only tweet to X API v2."""
    if not access_token:
        raise XClientError("Kein X-Access-Token gesetzt")

    payload = {"text": text}
    if media_ids:
        payload["media"] = {"media_ids": media_ids}

    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    try:
        logger.debug(f"X post_text: chars={len(text or '')} media_ids={len(media_ids or [])}")
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(f"{API_BASE}/tweets", json=payload, headers=headers)
            if r.status_code >= 400:
                logger.error(f"X API text post failed: {r.status_code} - {r.text}")
                raise XClientError(f"X API error {r.status_code}: {r.text[:200]}")

            result = r.json()
            logger.info(f"X tweet posted successfully: {result.get('data', {}).get('id', 'unknown')}")
            return result
    except httpx.RequestError as e:
        logger.exception(f"X API request error: {e}")
        raise XClientError(f"X API request failed: {str(e)}")

async def post_with_media(access_token: str, text: str, media_bytes: bytes) -> dict:
    """Post tweet with media to X."""
    if not access_token:
        raise XClientError("Kein X-Access-Token gesetzt")

    if not media_bytes or len(media_bytes) == 0:
        logger.warning("No media bytes provided, posting text only")
        return await post_text(access_token, text)

    try:
        # Upload media first
        media_id = await _upload_media(access_token, media_bytes)
        logger.info(f"Media uploaded with ID: {media_id}")

        # Post with media
        return await post_text(access_token, text, media_ids=[media_id])
    except XClientError:
        raise
    except Exception as e:
        logger.exception(f"X post with media error: {e}")
        raise XClientError(f"X post with media failed: {str(e)}")

async def get_user(access_token: str, username: str) -> dict:
    """Get user info from X."""
    if not access_token:
        raise XClientError("Kein X-Access-Token gesetzt")

    headers = {"Authorization": f"Bearer {access_token}"}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(f"{API_BASE}/users/by/username/{username}", headers=headers)
            if r.status_code >= 400:
                raise XClientError(f"X API error {r.status_code}: {r.text}")
            return r.json()
    except httpx.RequestError as e:
        logger.exception("X get_user request error")
        raise XClientError(f"X API request failed: {str(e)}")
