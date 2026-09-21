import asyncio
import html
import os
from datetime import datetime, timedelta, timezone

import psycopg
import resend
from jose import jwt
from psycopg.rows import dict_row

from ..config import APP_URL, DATABASE_URL, JWT_SECRET, RESEND_API_KEY, RESEND_FROM

_DL_TOKEN_HOURS = 48
_TMPL_DIR = os.path.join(os.path.dirname(__file__), "templates")


def _load_template(name: str, **kwargs) -> str:
  with open(os.path.join(_TMPL_DIR, name), encoding="utf-8") as f:
    out = f.read()
  for key, value in kwargs.items():
    out = out.replace(f"{{{{{key}}}}}", html.escape(str(value)))
  return out


def create_download_token(job_id: str) -> str:
  expire = datetime.now(timezone.utc) + timedelta(hours=_DL_TOKEN_HOURS)
  return jwt.encode(
    {"job_id": job_id, "exp": expire, "type": "dl"},
    JWT_SECRET,
    algorithm="HS256",
  )


def build_share_link(job_id: str) -> tuple[str, datetime]:
  """Génère un lien de partage/lecture pour ce job (valable _DL_TOKEN_HOURS).

  Utilisé à la fois par l'email "vidéo prête" et par l'endpoint de partage —
  même mécanisme, même durée, pour que les deux restent cohérents.
  """
  expire = datetime.now(timezone.utc) + timedelta(hours=_DL_TOKEN_HOURS)
  token = create_download_token(job_id)
  # Pointe sur vexia.studio (page produit), pas directement sur l'API :
  # la page se charge elle-même d'appeler orphee.olympe.center avec ce token.
  url = f"{APP_URL}/jobs/{job_id}/download?token={token}"
  return url, expire


def verify_download_token(token: str) -> str | None:
  try:
    payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    if payload.get("type") != "dl":
      return None
    return payload.get("job_id")
  except Exception:
    return None


async def _get_user_email(user_id: str) -> str | None:
  async with await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row) as conn:
    async with conn.cursor() as cur:
      await cur.execute("SELECT email FROM orphee_users WHERE id = %s", (user_id,))
      row = await cur.fetchone()
  return row["email"] if row and row.get("email") else None


def _fmt_duration(seconds: int) -> str:
  if seconds < 60:
    return f"{seconds}s"
  m, s = divmod(seconds, 60)
  return f"{m}m {s:02d}s" if s else f"{m}m"


async def send_video_ready(
  user_id: str, job_id: str, job_title: str, duration: int,
  file_size: int | None = None, clip_count: int = 0, template: str = "",
) -> None:
  if not RESEND_API_KEY:
    return

  email = await _get_user_email(user_id)
  if not email:
    return

  download_url, _ = build_share_link(job_id)
  html = _load_template("video_ready.html",
    job_title=job_title,
    duration=_fmt_duration(duration),
    download_url=download_url,
    app_url=APP_URL,
    job_id=job_id,
    file_size=file_size if file_size is not None else "",
    clip_count=clip_count,
    template=template,
  )

  resend.api_key = RESEND_API_KEY
  await asyncio.to_thread(
    resend.Emails.send,
    {
      "from": RESEND_FROM,
      "to": [email],
      "subject": f"✅ Votre vidéo est prête — {job_title}",
      "html": html,
    },
  )


async def send_video_failed(
  user_id: str, job_id: str, job_title: str, error: str,
  clip_count: int = 0, template: str = "",
) -> None:
  if not RESEND_API_KEY:
    return

  email = await _get_user_email(user_id)
  if not email:
    return

  error_display = (error[:200] + "…") if len(error) > 200 else error
  html = _load_template("video_failed.html",
    job_title=job_title,
    error=error_display,
    app_url=APP_URL,
    job_id=job_id,
    clip_count=clip_count,
    template=template,
  )

  resend.api_key = RESEND_API_KEY
  await asyncio.to_thread(
    resend.Emails.send,
    {
      "from": RESEND_FROM,
      "to": [email],
      "subject": f"❌ Erreur de rendu — {job_title}",
      "html": html,
    },
  )
