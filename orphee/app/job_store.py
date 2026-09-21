import asyncio
import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from typing import Optional

import psycopg
from psycopg.rows import dict_row

from .config import DATABASE_URL, STORAGE_ROOT

PENDING     = "pending"
DOWNLOADING = "downloading"
PROCESSING  = "processing"
DONE        = "done"
FAILED      = "failed"
CANCELLED   = "cancelled"

# Stockage en mémoire : job_id -> dict du job
_jobs: dict[str, dict] = {}

# Processus actifs : job_id -> asyncio.subprocess.Process
_processes: dict[str, asyncio.subprocess.Process] = {}

# Jobs annulés volontairement (survit à purge_job, contrairement à _jobs) —
# permet à la pipeline de fond de distinguer une annulation d'un vrai échec
# quand le sous-processus tué remonte une erreur après coup.
_cancelled_ids: set[str] = set()


def _job_dir(user_id: str, job_id: str) -> str:
  return os.path.join(STORAGE_ROOT, user_id, job_id)

def _job_file(user_id: str, job_id: str) -> str:
  return os.path.join(_job_dir(user_id, job_id), "job.json")

def final_path(user_id: str, job_id: str) -> str:
  return os.path.join(_job_dir(user_id, job_id), "final.mp4")


def cleanup_job_artifacts(user_id: str, job_id: str) -> None:
  """Supprime raw/, clips/ et overlays/ après un render réussi — garde final.mp4."""
  job_dir = _job_dir(user_id, job_id)
  for sub in ("raw", "clips", "overlays"):
    path = os.path.join(job_dir, sub)
    if os.path.isdir(path):
      shutil.rmtree(path, ignore_errors=True)


def purge_job(job_id: str) -> None:
  """Supprime le répertoire d'un job sur disque et le retire du store en mémoire."""
  job = _jobs.pop(job_id, None)
  if job:
    shutil.rmtree(_job_dir(job["user_id"], job_id), ignore_errors=True)


def create_job(user_id: str, title: Optional[str]) -> dict:
  """Crée un nouveau job et initialise son répertoire sur disque."""
  job_id = str(uuid.uuid4())
  job = {
    "job_id":     job_id,
    "user_id":    user_id,
    "status":     PENDING,
    "title":      title,
    "created_at": datetime.now(timezone.utc).isoformat(),
    "updated_at": datetime.now(timezone.utc).isoformat(),
    "error":      None,
  }

  for sub in ("raw", "clips"):
    os.makedirs(os.path.join(_job_dir(user_id, job_id), sub), exist_ok=True)

  _jobs[job_id] = job
  _persist(job)
  return job


def get_active_job_for_user(user_id: str) -> Optional[dict]:
  active = {PENDING, DOWNLOADING, PROCESSING}
  return next((j for j in _jobs.values() if j["user_id"] == user_id and j["status"] in active), None)

def get_active_jobs_for_user(user_id: str) -> list[dict]:
  active = {PENDING, DOWNLOADING, PROCESSING}
  return [j for j in _jobs.values() if j["user_id"] == user_id and j["status"] in active]


def get_job(job_id: str) -> Optional[dict]:
  return _jobs.get(job_id)


def update_job(job_id: str, **kwargs) -> Optional[dict]:
  """Met à jour les champs d'un job et persiste sur disque."""
  job = _jobs.get(job_id)
  if not job:
    return None
  kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()
  job.update(kwargs)
  _persist(job)
  return job


def register_process(job_id: str, process: asyncio.subprocess.Process) -> None:
  _processes[job_id] = process

def unregister_process(job_id: str) -> None:
  _processes.pop(job_id, None)

def get_process(job_id: str) -> Optional[asyncio.subprocess.Process]:
  return _processes.get(job_id)


def cancel_job(job_id: str) -> bool:
  """Annule un job en cours : tue le process actif et met à jour le statut."""
  job = _jobs.get(job_id)
  if not job or job["status"] in (DONE, FAILED, CANCELLED):
    return False

  _cancelled_ids.add(job_id)

  process = _processes.get(job_id)
  if process:
    try:
      process.terminate()
    except ProcessLookupError:
      pass
    unregister_process(job_id)

  update_job(job_id, status=CANCELLED)
  return True


def was_cancelled(job_id: str) -> bool:
  """Consomme le marqueur d'annulation — True une seule fois par job annulé."""
  if job_id in _cancelled_ids:
    _cancelled_ids.discard(job_id)
    return True
  return False


# ── Helpers DB appelés depuis les background tasks ────────────────────────────

async def db_insert_job(job_id: str, user_id: str, title: str) -> None:
  async with await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row) as conn:
    await conn.execute(
      "INSERT INTO orphee_jobs (id, user_id, title, status) VALUES (%s, %s, %s, 'pending')",
      (job_id, user_id, title),
    )

async def db_update_job_status(job_id: str, status: str, error: Optional[str] = None, file_size_bytes: Optional[int] = None, duration_seconds: Optional[int] = None) -> None:
  async with await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row) as conn:
    await conn.execute(
      "UPDATE orphee_jobs SET status = %s, error = %s, file_size_bytes = %s, duration_seconds = %s WHERE id = %s",
      (status, error, file_size_bytes, duration_seconds, job_id),
    )

async def db_get_job(job_id: str) -> Optional[dict]:
  async with await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row) as conn:
    async with conn.cursor() as cur:
      await cur.execute("SELECT * FROM orphee_jobs WHERE id = %s", (job_id,))
      row = await cur.fetchone()
  return dict(row) if row else None

async def db_delete_job(job_id: str) -> None:
  async with await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row) as conn:
    await conn.execute("DELETE FROM orphee_jobs WHERE id = %s", (job_id,))

async def db_cleanup_max_jobs(user_id: str, max_jobs: int) -> None:
  """Supprime les jobs DONE les plus anciens si le quota est dépassé."""
  import shutil as _shutil
  async with await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row) as conn:
    async with conn.cursor() as cur:
      await cur.execute(
        "SELECT id FROM orphee_jobs WHERE user_id = %s AND status = 'done' ORDER BY created_at ASC",
        (user_id,),
      )
      done_jobs = await cur.fetchall()

    excess = len(done_jobs) - max_jobs
    if excess <= 0:
      return

    for job in done_jobs[:excess]:
      job_dir = os.path.join(STORAGE_ROOT, user_id, str(job["id"]))
      if os.path.isdir(job_dir):
        _shutil.rmtree(job_dir, ignore_errors=True)

    ids = [str(j["id"]) for j in done_jobs[:excess]]
    async with conn.cursor() as cur:
      await cur.execute("DELETE FROM orphee_jobs WHERE id = ANY(%s)", (ids,))


async def db_increment_user_metrics(user_id: str, duration_seconds: int, clips_used: int) -> None:
  async with await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row) as conn:
    await conn.execute(
      """
      UPDATE orphee_users SET
        total_videos_created   = total_videos_created   + 1,
        total_duration_seconds = total_duration_seconds + %s,
        total_clips_used       = total_clips_used       + %s
      WHERE id = %s
      """,
      (duration_seconds, clips_used, user_id),
    )


async def db_increment_metrics(duration_seconds: int, clips_used: int) -> None:
  async with await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row) as conn:
    await conn.execute(
      """
      UPDATE orphee_metrics SET
        total_videos_created   = total_videos_created   + 1,
        total_duration_seconds = total_duration_seconds + %s,
        total_clips_used       = total_clips_used       + %s
      """,
      (duration_seconds, clips_used),
    )


def _persist(job: dict) -> None:
  """Écrit l'état du job dans job.json pour inspection manuelle."""
  try:
    with open(_job_file(job["user_id"], job["job_id"]), "w") as f:
      json.dump(job, f, indent=2)
  except OSError:
    pass
