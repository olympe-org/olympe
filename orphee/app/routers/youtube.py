from fastapi import APIRouter, Depends, HTTPException, Query

from ..auth import require_auth
from ..services import yt_dlp

router = APIRouter()


@router.get("/search")
async def search_youtube(
  q: str = Query(..., min_length=1),
  limit: int = Query(10, ge=1, le=25),
  user: dict = Depends(require_auth),
):
  """Cherche des vidéos YouTube (titre, durée, miniature) sans les télécharger."""
  try:
    results = await yt_dlp.search(q, limit=limit)
  except RuntimeError as e:
    raise HTTPException(status_code=502, detail=str(e))

  return {"results": results}
