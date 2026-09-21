import asyncio
import json
import os
import re
import urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import AsyncGenerator, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from ..auth import require_auth
import shutil

from ..config import STORAGE_ROOT
from ..db import get_db
from ..job_store import (
  CANCELLED, DONE, FAILED,
  cancel_job, create_job, db_cleanup_max_jobs, db_delete_job, db_get_job, db_insert_job,
  db_increment_metrics, db_increment_user_metrics, db_update_job_status,
  final_path, get_active_job_for_user, get_job, purge_job, update_job,
)
from ..services import ffmpeg
from ..services.email import build_share_link, send_video_failed, send_video_ready, verify_download_token

router = APIRouter()


# ── Schémas ──────────────────────────────────────────────────────────────────

class TitleStyle(BaseModel):
  border: Optional[int] = None
  color: Optional[str] = None
  font: Optional[str] = None
  size: Optional[int] = None
  opacity: Optional[float] = None


class SubtitleStyle(BaseModel):
  border: Optional[int] = None
  color: Optional[str] = None
  font: Optional[str] = None
  size: Optional[int] = None
  opacity: Optional[float] = None


class ClipTitleStyle(BaseModel):
  animation: str = "fade"
  border: Optional[int] = None
  color: Optional[str] = None
  font: Optional[str] = None
  position: str = "left"
  size: Optional[int] = None
  opacity: Optional[float] = None


class VideoTitle(BaseModel):
  first: str
  second: Optional[str] = None
  titleStyle: Optional[TitleStyle] = None
  subtitle: Optional[str] = None
  subtitleStyle: Optional[SubtitleStyle] = None


class ClipIdStyle(BaseModel):
  border: Optional[int] = None
  color: Optional[str] = None
  font: Optional[str] = None
  size: Optional[int] = None
  opacity: Optional[float] = None


class ClipItem(BaseModel):
  id: str
  idStyle: Optional[ClipIdStyle] = None
  url: str
  title: str
  subtitle: Optional[str] = None
  subtitleStyle: Optional[ClipTitleStyle] = None
  duration: int
  claude: bool = False
  start_time: Optional[str] = None
  titleStyle: Optional[ClipTitleStyle] = None


class HighlightActive(BaseModel):
  active: bool = False
  inactiveColor: str = "0x888888"


class RenderRequest(BaseModel):
  title: VideoTitle
  job_name: Optional[str] = None
  template: str = "top"
  highlightActive: Optional[HighlightActive] = None
  teaserTop: bool = False
  smoothTransition: Optional[dict] = None
  background: str = "video"
  watermark: Optional[dict] = None
  spacing: Optional[int] = None
  videoMargin: Optional[int] = None
  data: list[ClipItem]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/render", status_code=201)
async def create_render_job(
  body: RenderRequest,
  background_tasks: BackgroundTasks,
  user: dict = Depends(require_auth),
):
  """Crée un job de rendu multi-clips (pipeline 9:16)."""
  if not body.data:
    raise HTTPException(status_code=400, detail="Le tableau data est vide.")

  if body.template not in ("top", "classic", "minimal", "expanded"):
    raise HTTPException(status_code=400, detail=f"template invalide : '{body.template}'.")

  for item in body.data:
    if not (item.start_time and item.start_time.strip()) and not item.claude:
      raise HTTPException(
        status_code=400,
        detail=f"Clip id={item.id} doit avoir soit un start_time, soit claude=true.",
      )

  user_id = str(user["id"])

  active = get_active_job_for_user(user_id)
  if active:
    raise HTTPException(
      status_code=409,
      detail={
        "message": "Un job est déjà en cours, impossible d'en lancer un nouveau.",
        "job_id": active["job_id"],
        "status": active["status"],
      },
    )

  custom_name = (body.job_name or "").strip()
  if custom_name:
    # Retire les caractères qui casseraient un nom de fichier / header HTTP
    # (le titre finit dans le nom du fichier téléchargé et dans les emails).
    slug = re.sub(r'[\r\n"/\\]', "", custom_name)[:100]
  else:
    now = datetime.now(ZoneInfo("Europe/Paris"))
    slug = f"{body.template}-{user['username']}-{now.strftime('%Y-%m-%d-%Hh%M')}"

  job = create_job(user_id=user_id, title=slug)
  await db_insert_job(job["job_id"], user_id, slug)

  payload = body.model_dump()
  background_tasks.add_task(_run_render_pipeline, job["job_id"], user_id, user["max_jobs"], payload)

  return {
    "job_id":     job["job_id"],
    "status":     job["status"],
    "title":      job["title"],
    "created_at": job["created_at"],
    "error":      job["error"],
  }


@router.get("/last")
async def get_last_job(
  user: dict = Depends(require_auth),
  conn = Depends(get_db),
):
  """Retourne le dernier job de l'utilisateur connecté."""
  async with conn.cursor() as cur:
    await cur.execute(
      """
      SELECT id, title, status, created_at, updated_at, error
      FROM orphee_jobs
      WHERE user_id = %s
      ORDER BY created_at DESC
      LIMIT 1
      """,
      (str(user["id"]),),
    )
    job = await cur.fetchone()

  if not job:
    raise HTTPException(status_code=404, detail="Aucun job trouvé.")

  return {
    "job_id":     str(job["id"]),
    "title":      job["title"],
    "status":     job["status"],
    "created_at": job["created_at"],
    "updated_at": job["updated_at"],
  }


@router.get("/{job_id}/stream")
async def stream_job(job_id: str, user: dict = Depends(require_auth)):
  """SSE — suit l'avancement d'un job en temps réel jusqu'à sa fin."""
  job = get_job(job_id)
  if not job:
    raise HTTPException(status_code=404, detail="Job introuvable.")
  if job["user_id"] != str(user["id"]):
    raise HTTPException(status_code=403, detail="Accès refusé.")

  return StreamingResponse(
    _sse_generator(job_id),
    media_type="text/event-stream",
    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
  )


_RANGE_CHUNK = 1024 * 1024  # 1 Mo


def _iter_file_range(path: str, start: int, end: int, chunk_size: int = _RANGE_CHUNK):
  with open(path, "rb") as f:
    f.seek(start)
    remaining = end - start + 1
    while remaining > 0:
      chunk = f.read(min(chunk_size, remaining))
      if not chunk:
        break
      remaining -= len(chunk)
      yield chunk


@router.get("/{job_id}/download")
async def download_job(
  job_id: str,
  request: Request,
  token: Optional[str] = None,
  credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False)),
  conn = Depends(get_db),
):
  """Sert le final.mp4 d'un job terminé (auth JWT ou token de partage/lecture).

  Accès par token (aperçu in-app ou lien de partage) : lecture inline (streaming).
  Accès par JWT (bouton "Télécharger" explicite) : téléchargement forcé.
  """
  disposition = "attachment"
  if token:
    verified_job_id = verify_download_token(token)
    if not verified_job_id or verified_job_id != job_id:
      raise HTTPException(status_code=403, detail="Token de téléchargement invalide ou expiré.")
    disposition = "inline"
  elif credentials:
    user = await require_auth(credentials, conn)
    job = get_job(job_id) or await db_get_job(job_id)
    if not job:
      raise HTTPException(status_code=404, detail="Job introuvable.")
    if str(job["user_id"]) != str(user["id"]) and not user["is_admin"]:
      raise HTTPException(status_code=403, detail="Accès refusé.")
  else:
    raise HTTPException(status_code=401, detail="Authentification requise.")

  job = get_job(job_id) or await db_get_job(job_id)
  if not job:
    raise HTTPException(status_code=404, detail="Job introuvable.")
  if job["status"] != DONE:
    raise HTTPException(status_code=409, detail=f"La vidéo n'est pas encore prête (statut : {job['status']}).")

  user_id = str(job["user_id"])
  path = final_path(user_id, job_id)
  if not os.path.exists(path):
    raise HTTPException(status_code=404, detail="Fichier final.mp4 introuvable sur le disque.")

  filename = f"{job['title']}_{job_id[:8]}.mp4"
  content_disposition = f"{disposition}; filename=\"{filename}\""
  file_size = os.path.getsize(path)
  # Titre encodé (accents/emojis invalides tels quels dans un header HTTP) —
  # décoder avec decodeURIComponent() côté front.
  job_title_header = urllib.parse.quote(job["title"])

  range_header = request.headers.get("range")
  if range_header:
    match = re.match(r"bytes=(\d+)-(\d*)", range_header)
    if not match:
      raise HTTPException(status_code=416, detail="Range invalide.")
    start = int(match.group(1))
    end = int(match.group(2)) if match.group(2) else file_size - 1
    end = min(end, file_size - 1)
    if start > end or start >= file_size:
      raise HTTPException(status_code=416, detail="Range invalide.")

    return StreamingResponse(
      _iter_file_range(path, start, end),
      status_code=206,
      media_type="video/mp4",
      headers={
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(end - start + 1),
        "Content-Disposition": content_disposition,
        "X-Job-Title": job_title_header,
      },
    )

  return FileResponse(
    path,
    media_type="video/mp4",
    filename=filename,
    headers={
      "Content-Disposition": content_disposition,
      "Accept-Ranges": "bytes",
      "X-Job-Title": job_title_header,
    },
  )


@router.get("/{job_id}/share-link")
async def get_share_link(job_id: str, user: dict = Depends(require_auth)):
  """Génère un lien de lecture/partage à la demande (valable 48h).

  Utilisé en silence par l'aperçu in-app (pour obtenir une URL utilisable
  dans une balise <video>) et explicitement par le bouton "Partager"/QR code.
  """
  job = get_job(job_id) or await db_get_job(job_id)
  if not job:
    raise HTTPException(status_code=404, detail="Job introuvable.")
  if str(job["user_id"]) != str(user["id"]) and not user["is_admin"]:
    raise HTTPException(status_code=403, detail="Accès refusé.")
  if job["status"] != DONE:
    raise HTTPException(status_code=409, detail=f"La vidéo n'est pas encore prête (statut : {job['status']}).")

  url, expires_at = build_share_link(job_id)
  return {"url": url, "expires_at": expires_at.isoformat()}


@router.delete("/{job_id}")
async def delete_job(job_id: str, user: dict = Depends(require_auth)):
  """Supprime un job (actif ou terminé). User = ses jobs uniquement, admin = tous les jobs."""
  mem_job = get_job(job_id)
  db_job  = mem_job or await db_get_job(job_id)
  if not db_job:
    raise HTTPException(status_code=404, detail="Job introuvable.")
  if str(db_job["user_id"]) != str(user["id"]) and not user["is_admin"]:
    raise HTTPException(status_code=403, detail="Accès refusé.")

  if mem_job:
    cancel_job(job_id)
    purge_job(job_id)
  else:
    job_dir = os.path.join(STORAGE_ROOT, str(db_job["user_id"]), job_id)
    if os.path.isdir(job_dir):
      shutil.rmtree(job_dir, ignore_errors=True)

  await db_delete_job(job_id)
  return {"detail": "Job supprimé."}


# ── Pipeline ──────────────────────────────────────────────────────────────────

async def _run_render_pipeline(
  job_id: str,
  user_id: str,
  max_jobs: int,
  payload: dict,
) -> None:
  try:
    await ffmpeg.render_video(job_id, user_id, payload)

    path = final_path(user_id, job_id)
    file_size = os.path.getsize(path) if os.path.exists(path) else None
    clips = payload.get("data", [])
    duration = sum(c.get("duration", 0) for c in clips)

    update_job(job_id, status=DONE, message="Vidéo prête !")
    await db_update_job_status(job_id, DONE, file_size_bytes=file_size, duration_seconds=duration)
    await db_increment_metrics(duration_seconds=duration, clips_used=len(clips))
    await db_increment_user_metrics(user_id, duration_seconds=duration, clips_used=len(clips))
    await db_cleanup_max_jobs(user_id, max_jobs)
    job = get_job(job_id)
    await send_video_ready(user_id, job_id, job["title"] if job else job_id, duration)
  except asyncio.CancelledError:
    pass
  except Exception as e:
    print(f"[pipeline] job={job_id} FAILED: {e}")
    update_job(job_id, status=FAILED, error=str(e), message=f"Erreur : {e}")
    await db_update_job_status(job_id, FAILED, error=str(e))
    job = get_job(job_id)
    await send_video_failed(user_id, job_id, job["title"] if job else job_id, str(e))
    # Nettoie les fichiers disque mais garde le job en mémoire (status=FAILED)
    # pour que le générateur SSE puisse remonter l'erreur avant la fin de la connexion.
    job_dir = os.path.join(STORAGE_ROOT, user_id, job_id)
    if os.path.isdir(job_dir):
      shutil.rmtree(job_dir, ignore_errors=True)


# ── Générateur SSE ────────────────────────────────────────────────────────────

async def _sse_generator(job_id: str) -> AsyncGenerator[str, None]:
  terminal = {DONE, FAILED, CANCELLED}

  while True:
    job = get_job(job_id)
    if not job:
      yield _sse_event({"error": "Job introuvable."})
      break

    yield _sse_event(_job_view(job))

    if job["status"] in terminal:
      break

    await asyncio.sleep(0.5)


def _job_view(job: dict) -> dict:
  view = {
    "job_id":     job["job_id"],
    "status":     job["status"],
    "title":      job["title"],
    "created_at": job["created_at"],
    "updated_at": job["updated_at"],
    "error":      job["error"],
  }
  if job.get("clips") is not None:
    view["clips"] = job["clips"]
  if job.get("message") is not None:
    view["message"] = job["message"]
  return view


def _sse_event(data: dict) -> str:
  return f"data: {json.dumps(data)}\n\n"
