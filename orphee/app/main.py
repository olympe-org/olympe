import os

import psycopg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from psycopg.rows import dict_row

from .config import DATABASE_URL
from .routers import admin, auth, video, youtube

# Orphée — API de génération automatique de vidéos musicales
app = FastAPI(title="Orphée API", version="0.1.0")

origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
if origins:
  app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
  )

@app.get("/health")
def health():
  return {"status": "ok"}

@app.get("/metrics")
async def metrics():
  async with await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row) as conn:
    async with conn.cursor() as cur:
      await cur.execute("SELECT total_videos_created, total_duration_seconds, total_clips_used, money_earned FROM orphee_metrics LIMIT 1")
      row = await cur.fetchone()
  if not row:
    return {"total_videos_created": 0, "total_duration_seconds": 0, "total_clips_used": 0}
  return dict(row)

# Routers
app.include_router(auth.router,  prefix="/auth",  tags=["auth"])
app.include_router(admin.router, prefix="/admin", tags=["admin"])
app.include_router(video.router, prefix="/jobs",  tags=["jobs"])
app.include_router(youtube.router, prefix="/youtube", tags=["youtube"])
