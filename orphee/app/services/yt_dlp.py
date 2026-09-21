import asyncio
import json
import os

from ..job_store import register_process, unregister_process, update_job, DOWNLOADING

_FORMAT = (
  "best[protocol*=m3u8][height<=720]"
  "/bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]"
  "/bestvideo[height<=720]+bestaudio"
  "/best[height<=720][ext=mp4]"
  "/best[height<=720]"
)


def _parse_seconds(time_str: str) -> float:
  parts = [float(p) for p in time_str.strip().split(":")]
  if len(parts) == 3:
    return parts[0] * 3600 + parts[1] * 60 + parts[2]
  if len(parts) == 2:
    return parts[0] * 60 + parts[1]
  return parts[0]


def _fmt_time(seconds: float) -> str:
  h = int(seconds // 3600)
  m = int((seconds % 3600) // 60)
  s = seconds % 60
  return f"{h:02d}:{m:02d}:{s:06.3f}"


async def _run_ytdlp(cmd: list[str], job_id: str) -> tuple[int, bytes]:
  process = await asyncio.create_subprocess_exec(
    *cmd,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
  )
  register_process(job_id, process)
  _, stderr = await process.communicate()
  unregister_process(job_id)
  return process.returncode, stderr


async def download(job_id: str, url: str, output_dir: str,
                   start_time: str | None = None,
                   duration: int | None = None) -> tuple[str, bool]:
  """Télécharge une vidéo via yt-dlp (YouTube, Instagram, TikTok, Vimeo...)."""
  update_job(job_id, status=DOWNLOADING, message="Téléchargement en cours...")

  output_template = os.path.join(output_dir, "%(title)s.%(ext)s")

  base_cmd = [
    "yt-dlp",
    "--no-playlist",
    "--format", _FORMAT,
    "--merge-output-format", "mp4",
    "--retries", "5",
    "--fragment-retries", "5",
    "--output", output_template,
  ]

  proxy = os.getenv("YTDLP_PROXY", "")
  if proxy:
    base_cmd += ["--proxy", proxy]

  sections_args = []
  if start_time is not None and duration is not None:
    start_s = _parse_seconds(start_time)
    end_s = start_s + duration + 2
    sections_args = ["--download-sections", f"*{_fmt_time(start_s)}-{_fmt_time(end_s)}"]

  def _clear_dir():
    for f in os.listdir(output_dir):
      os.remove(os.path.join(output_dir, f))

  sections_used = False
  returncode, stderr = await _run_ytdlp(base_cmd + sections_args + [url], job_id)

  if returncode == 0 and sections_args:
    sections_used = True
  elif returncode != 0 and sections_args:
    print("[yt-dlp] --download-sections a échoué, retry sans sections")
    _clear_dir()
    returncode, stderr = await _run_ytdlp(base_cmd + [url], job_id)

  if returncode != 0:
    error = stderr.decode().strip().splitlines()[-1] if stderr else "Erreur inconnue"
    raise RuntimeError(f"yt-dlp a échoué : {error}")

  files = [f for f in os.listdir(output_dir) if f.endswith(".mp4")]
  if not files:
    raise RuntimeError("yt-dlp n'a produit aucun fichier mp4.")

  return os.path.join(output_dir, files[0]), sections_used


async def search(query: str, limit: int = 10) -> list[dict]:
  """Cherche des vidéos YouTube via yt-dlp (pas de téléchargement)."""
  cmd = [
    "yt-dlp",
    f"ytsearch{limit}:{query}",
    "--flat-playlist",
    "--dump-json",
    "--no-warnings",
  ]

  proxy = os.getenv("YTDLP_PROXY", "")
  if proxy:
    cmd += ["--proxy", proxy]

  process = await asyncio.create_subprocess_exec(
    *cmd,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
  )
  stdout, stderr = await process.communicate()

  if process.returncode != 0:
    error = stderr.decode().strip().splitlines()[-1] if stderr else "Erreur inconnue"
    raise RuntimeError(f"yt-dlp search a échoué : {error}")

  results = []
  for line in stdout.decode().strip().splitlines():
    if not line:
      continue
    entry = json.loads(line)
    results.append({
      "id": entry.get("id"),
      "title": entry.get("title"),
      "url": entry.get("url") or f"https://www.youtube.com/watch?v={entry.get('id')}",
      "duration": entry.get("duration"),
      "thumbnail": entry.get("thumbnails", [{}])[-1].get("url") if entry.get("thumbnails") else None,
      "channel": entry.get("channel") or entry.get("uploader"),
    })

  return results
