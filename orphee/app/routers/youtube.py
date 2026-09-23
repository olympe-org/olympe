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
  """Cherche des vidéos YouTube (titre, durée, vues) sans les télécharger.

  Renvoie un tableau au format Invidious (type/videoId/title/author/
  lengthSeconds/viewCountText), pour que le front n'ait qu'un seul schéma
  à gérer entre ce backend et une instance Invidious publique.
  """
  try:
    return await yt_dlp.search(q, limit=limit)
  except RuntimeError as e:
    raise HTTPException(status_code=502, detail=str(e))
