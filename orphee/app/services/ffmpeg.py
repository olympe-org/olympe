import asyncio
import os
import time

from ..job_store import (
  register_process, unregister_process, update_job,
  final_path, cleanup_job_artifacts, PROCESSING,
)
from . import text_render

# Fonts disponibles — clé utilisée dans titleStyle.font
_FONTS = {
  "montserrat":        "/app/fonts/Montserrat-Bold.ttf",
  "montserrat-light":  "/app/fonts/Montserrat-Light.ttf",
  "montserrat-medium": "/app/fonts/Montserrat-Medium.ttf",
  "bebas":             "/app/fonts/BebasNeue-Regular.ttf",
  "inter":             "/app/fonts/Inter-Regular.ttf",
  "inter-medium":      "/app/fonts/Inter-Medium.ttf",
  "inter-semibold":    "/app/fonts/Inter-SemiBold.ttf",
  "dejavu":            "/app/fonts/DejaVuSans-Bold.ttf",
  "helvetica":         "/app/fonts/HelveticaNow-Regular.ttf",
  "helvetica-bold":    "/app/fonts/HelveticaNow-Bold.ttf",
  "helvetica-black":   "/app/fonts/HelveticaNow-Black.ttf",
}
_FONT_DEFAULT = _FONTS["dejavu"]

def _resolve_font(name: str | None) -> str:
  if not name:
    return _FONT_DEFAULT
  return _FONTS.get(name, _FONT_DEFAULT)


# Formats supportés (pipeline legacy)
FORMATS = {
  "portrait":  (1080, 1920),
  "landscape": (1920, 1080),
}

# Dimensions de la vidéo finale
OUT_W, OUT_H = 1080, 1920

# Vidéo principale (foreground)
FG_H = 700


# Assombrissement du fond flouté
BG_BRIGHTNESS = 0.3

# Titres globaux (header) — valeurs par défaut
TITLE_FONTSIZE    = 80
SUBTITLE_FONTSIZE = 36
TITLE_BORDER      = 6
SUBTITLE_BORDER   = 4
TITLE1_Y          = 120
TITLE2_Y          = 210
SUBTITLE_Y        = 290

# Template "top" — liste de rangs — valeurs par défaut
RANK_FONTSIZE          = 64
RANK_X                 = 60
RANK_BORDER            = 6
CLIP_TITLE_FONTSIZE    = 44
CLIP_TITLE_X           = 180
CLIP_TITLE_BORDER      = 5
CLIP_SUBTITLE_FONTSIZE = 32
CLIP_SUBTITLE_BORDER   = 4

# Sliding window (template "top", n > WINDOW_SIZE)
WINDOW_SIZE = 5    # nombre max d'items affichés simultanément
SLIDE_DUR   = 0.4  # durée de l'animation de glissement (secondes)

# Template "classic"
CLASSIC_FG_Y          = 780   # vidéo commence à y=780
CLASSIC_CLIP_ZONE_TOP = 350   # bas approximatif du header global

# Template "minimal"
MINIMAL_FG_Y = 610  # (OUT_H - FG_H) // 2 = (1920-700)//2 — vidéo centrée

# Template "expanded"
EXPANDED_TOP    = 250   # zone texte en haut
EXPANDED_BOTTOM = 250   # zone vide en bas
EXPANDED_FG_Y   = EXPANDED_TOP
EXPANDED_FG_H   = OUT_H - EXPANDED_TOP - EXPANDED_BOTTOM  # 1620


# ── Primitives ────────────────────────────────────────────────────────────────

async def _run(job_id: str, *args: str, label: str = "ffmpeg") -> None:
  """Lance une commande ffmpeg et lève une erreur si elle échoue."""
  start = time.monotonic()
  process = await asyncio.create_subprocess_exec(
    "ffmpeg", "-y", *args,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
  )
  register_process(job_id, process)
  _, stderr = await process.communicate()
  unregister_process(job_id)
  print(f"[ffmpeg][{job_id}] {label} terminé en {time.monotonic() - start:.2f}s (code={process.returncode})")

  if process.returncode != 0:
    lines = stderr.decode().strip().splitlines() if stderr else []
    error = "\n".join(lines[-5:]) if lines else "Erreur ffmpeg inconnue"
    raise RuntimeError(f"ffmpeg a échoué : {error}")


# ── Pipeline legacy (single-clip) ─────────────────────────────────────────────

async def extract_clip(job_id: str, source: str, start_time: str, duration: int, output_path: str) -> str:
  """Extrait un clip à partir du timestamp et de la durée demandés."""
  update_job(job_id, message=f"Extraction du clip à {start_time}...")

  await _run(
    job_id,
    "-ss", start_time,
    "-i", source,
    "-t", str(duration),
    "-c:v", "libx264",
    "-preset", "veryfast",
    "-r", "30",
    "-vsync", "cfr",
    "-crf", "14",
    "-af", "loudnorm=linear=true:I=-14",
    "-c:a", "aac",
    "-avoid_negative_ts", "make_zero",
    output_path,
    label="extract_clip",
  )
  return output_path


async def assemble(
  job_id: str,
  clip_path: str,
  title: str,
  artist: str,
  fmt: str,
  output_path: str,
) -> str:
  """Redimensionne au format choisi, ajoute le texte titre/artiste, produit le final.mp4."""
  if fmt not in FORMATS:
    raise ValueError(f"Format invalide : '{fmt}'. Valeurs acceptées : {list(FORMATS.keys())}")

  w, h = FORMATS[fmt]
  update_job(job_id, status=PROCESSING, message=f"Assemblage en format {fmt} ({w}x{h})...")

  safe_title  = title.replace("'", "\\'")  if title  else ""
  safe_artist = artist.replace("'", "\\'") if artist else ""

  font_size_title  = 60
  font_size_artist = 44
  padding          = 80

  vf_parts = [
    f"scale={w}:{h}:force_original_aspect_ratio=increase",
    f"crop={w}:{h}",
  ]

  if safe_title:
    vf_parts.append(
      f"drawtext=fontfile='{_FONT_DEFAULT}':text='{safe_title}'"
      f":fontsize={font_size_title}:fontcolor=white"
      f":x=(w-text_w)/2:y={padding}"
      f":shadowcolor=black:shadowx=2:shadowy=2"
    )

  if safe_artist:
    vf_parts.append(
      f"drawtext=fontfile='{_FONT_DEFAULT}':text='{safe_artist}'"
      f":fontsize={font_size_artist}:fontcolor=white"
      f":x=(w-text_w)/2:y=h-text_h-{padding}"
      f":shadowcolor=black:shadowx=2:shadowy=2"
    )

  await _run(
    job_id,
    "-i", clip_path,
    "-vf", ",".join(vf_parts),
    "-c:v", "libx264",
    "-preset", "fast",
    "-crf", "18",
    "-c:a", "aac",
    "-b:a", "192k",
    output_path,
  )
  return output_path


# ── Pipeline multi-clips ───────────────────────────────────────────────────────

def build_filter_complex(
  data: list[dict],
  title: dict,
  template: str,
  overlay_dir: str,
  highlight: dict | None = None,
  teaser_top: bool = False,
  smooth_transition: dict | None = None,
  background: str = "video",
  watermark: dict | None = None,
  spacing: int | None = None,
  video_margin: int = 0,
) -> tuple[str, list[str]]:
  """Construit la valeur de -filter_complex pour une vidéo 9:16 (1080x1920).

  template : "top" | "classic" | "minimal"
  Retourne (filter_complex_string, [chemins PNG supplémentaires]).
  """
  # ── fg_y / fg_h selon le template ────────────────────────────────────────────
  if template == "classic":
    fg_y = CLASSIC_FG_Y
    fg_h = FG_H
  elif template == "minimal":
    fg_y = MINIMAL_FG_Y
    fg_h = FG_H
  elif template == "expanded":
    fg_y = EXPANDED_FG_Y
    fg_h = EXPANDED_FG_H
  else:  # top
    fg_y = 960
    fg_h = FG_H

  # spacing : gap entre le bloc title/subtitle et la vidéo (classic/minimal)
  # ou gap interne entre title et subtitle (expanded)
  _spacing: int = spacing if spacing is not None else (20 if template == "expanded" else 60)

  # teaserTop : data[0] n'a pas de clip vidéo (top uniquement)
  n_clips       = len(data) - 1 if teaser_top else len(data)
  parts:        list[str] = []
  extra_inputs: list[str] = []
  lbl_ctr  = [0]
  inp_ctr  = [n_clips]   # inputs 0..n_clips-1 = clips ; PNG inputs start at n_clips

  def _next_lbl() -> str:
    lbl_ctr[0] += 1
    return f"v{lbl_ctr[0]}"

  def _next_inp() -> int:
    n = inp_ctr[0]
    inp_ctr[0] += 1
    return n

  def _v(d: dict, key: str, default):
    v = d.get(key)
    return v if v is not None else default

  def _overlay_text(
    in_lbl: str,
    text: str,
    fontsize: int,
    border_w: int,
    x_mode: str,           # "center" | "fixed:<px>"
    y_px: int,
    font: str | None = None,
    color: str = "0xFFFFFF",
    appear_at: float | None = None,
    animation: str = "fade",
    enable: str = "",
    inactive_color: str | None = None,
    inactive_at: float | None = None,
    always_visible: bool = False,
    opacity: float = 1.0,
    active_until: float | None = None,  # coupure propre sans changement de couleur
  ) -> str:
    font_path  = _resolve_font(font)
    ts         = appear_at if appear_at is not None else 0.0
    alpha_proc = f",colorchannelmixer=aa={opacity:.2f}" if opacity < 1.0 else ""

    def _add_png(col: str, txt: str = text) -> tuple[int, int]:
      k        = _next_inp()
      png_path = os.path.join(overlay_dir, f"text_{k}.png")
      png_w, _ = text_render.render_text_png(txt, font_path, fontsize, col, border_w, png_path)
      extra_inputs.append(png_path)
      x = (OUT_W - png_w) // 2 if x_mode == "center" else int(x_mode.split(":")[1])
      return k, x

    def _proc(k: int, alpha_filter: str) -> str:
      p = _next_lbl()
      parts.append(f"[{k}:v]format=rgba{alpha_filter}{alpha_proc}[{p}]")
      return p

    def _ov(cur: str, p: str, x_expr: str, y_expr: str, en: str) -> str:
      out = _next_lbl()
      parts.append(f"[{cur}][{p}]overlay=x={x_expr}:y={y_expr}{en}[{out}]")
      return out

    cur_lbl = in_lbl

    # ── Mode ID : toujours visible, couleur qui change ────────────────────────
    if always_visible and inactive_color:
      if inactive_at is not None:
        k_ina, x_ina = _add_png(inactive_color)
        cur_lbl = _ov(cur_lbl, _proc(k_ina, ",colorchannelmixer=aa=0.6"),
                      str(x_ina), str(y_px),
                      f":enable='not(between(t,{ts},{inactive_at}))'")
        k_act, x_act = _add_png(color)
        cur_lbl = _ov(cur_lbl, _proc(k_act, ""),
                      str(x_act), str(y_px),
                      f":enable='between(t,{ts},{inactive_at})'")
      else:
        # teaser : inactif avant ts, actif à partir de ts
        k_ina, x_ina = _add_png(inactive_color)
        cur_lbl = _ov(cur_lbl, _proc(k_ina, ",colorchannelmixer=aa=0.6"),
                      str(x_ina), str(y_px),
                      f":enable='lt(t,{ts})'")
        k_act, x_act = _add_png(color)
        cur_lbl = _ov(cur_lbl, _proc(k_act, ""),
                      str(x_act), str(y_px),
                      f":enable='gte(t,{ts})'")
      return cur_lbl

    # ── Typewriter ────────────────────────────────────────────────────────────
    if animation == "typewriter" and appear_at is not None:
      chars = list(text)
      N     = max(len(chars), 1)
      dt    = 0.5 / N
      for j in range(N):
        k, x = _add_png(color, text[:j + 1])
        p    = _proc(k, "")
        t_on = ts + j * dt
        if j < N - 1:
          t_off = ts + (j + 1) * dt
          en = f":enable='between(t,{t_on:.4f},{t_off:.4f})'"
        else:
          cutoff = active_until if active_until is not None else (inactive_at if (inactive_color and inactive_at) else None)
          en = f":enable='between(t,{t_on:.4f},{cutoff})'" if cutoff is not None \
               else f":enable='gte(t,{t_on:.4f})'"
        cur_lbl = _ov(cur_lbl, p, str(x), str(y_px), en)
      if inactive_color and inactive_at is not None:
        k_ina, x_ina = _add_png(inactive_color)
        cur_lbl = _ov(cur_lbl, _proc(k_ina, ",colorchannelmixer=aa=0.6"),
                      str(x_ina), str(y_px), f":enable='gte(t,{inactive_at})'")
      return cur_lbl

    # ── Animations standard (fade, none, slide-left, slide-bottom) ────────────
    if animation == "fade" and appear_at is not None:
      alpha_filter = f",fade=t=in:st={ts}:d=0.5:alpha=1"
      active_en    = ""
    elif animation in ("slide-left", "slide-bottom") and appear_at is not None:
      alpha_filter = f",fade=t=in:st={ts}:d=0.3:alpha=1"
      active_en    = ""
    elif animation == "none" and appear_at is not None:
      alpha_filter = ""
      active_en    = f":enable='gte(t,{ts})'"
    else:
      alpha_filter = ""
      active_en    = f":enable='{enable}'" if enable else ""

    # inactive_color override (highlight)
    if inactive_color and inactive_at is not None:
      active_en = f":enable='between(t,{ts},{inactive_at})'"

    # active_until : coupure propre sans changement de couleur
    if active_until is not None and not (inactive_color and inactive_at is not None):
      if appear_at is not None:
        active_en = f":enable='between(t,{ts:.4f},{active_until:.4f})'"
      else:
        active_en = f":enable='lte(t,{active_until:.4f})'"

    k_act, x_num = _add_png(color)
    p_act = _proc(k_act, alpha_filter)

    if animation == "slide-left" and appear_at is not None:
      x_expr = f"'if(lte(t-{ts},0.3),{x_num}-(0.3-(t-{ts}))/0.3*40,{x_num})'"
      y_expr = str(y_px)
    elif animation == "slide-bottom" and appear_at is not None:
      x_expr = str(x_num)
      y_expr = f"'if(lte(t-{ts},0.3),{y_px}+(0.3-(t-{ts}))/0.3*40,{y_px})'"
    else:
      x_expr = str(x_num)
      y_expr = str(y_px)

    cur_lbl = _ov(cur_lbl, p_act, x_expr, y_expr, active_en)

    if inactive_color and inactive_at is not None:
      k_ina, x_ina = _add_png(inactive_color)
      cur_lbl = _ov(cur_lbl, _proc(k_ina, ",colorchannelmixer=aa=0.6"),
                    str(x_ina), str(y_px), f":enable='gte(t,{inactive_at})'")

    return cur_lbl

  # ── Traitement bg/fg par clip ─────────────────────────────────────────────────
  bg_lbls: list[str] = []
  fg_lbls: list[str] = []

  bg_is_color = background != "video"
  bg_hex = "#" + background[2:] if bg_is_color else None

  for i in range(n_clips):
    bg_lbl = _next_lbl()
    if bg_is_color:
      parts.append(
        f"color=c={bg_hex}:s={OUT_W}x{OUT_H}:r=30,format=yuv420p[{bg_lbl}]"
      )
      s2 = _next_lbl()
      parts.append(f"[{i}:v]null[{s2}]")
    else:
      s1, s2 = _next_lbl(), _next_lbl()
      parts.append(f"[{i}:v]split=2[{s1}][{s2}]")
      parts.append(
        f"[{s1}]"
        f"scale={OUT_W}:{OUT_H}:force_original_aspect_ratio=increase,"
        f"crop={OUT_W}:{OUT_H},setsar=1,"
        f"boxblur=15:2,"
        f"colorchannelmixer=rr={BG_BRIGHTNESS}:gg={BG_BRIGHTNESS}:bb={BG_BRIGHTNESS},"
        f"format=yuv420p"
        f"[{bg_lbl}]"
      )
    bg_lbls.append(bg_lbl)

    fg_lbl = _next_lbl()
    fg_w   = OUT_W - 2 * video_margin
    parts.append(
      f"[{s2}]"
      f"scale={fg_w}:{fg_h}:force_original_aspect_ratio=increase,"
      f"crop={fg_w}:{fg_h},setsar=1,format=yuv420p"
      f"[{fg_lbl}]"
    )
    fg_lbls.append(fg_lbl)

  # ── Concat bg, fg, audio ──────────────────────────────────────────────────────
  bg_out = _next_lbl()
  parts.append("".join(f"[{l}]" for l in bg_lbls) + f"concat=n={n_clips}:v=1[{bg_out}]")

  fg_out = _next_lbl()
  parts.append("".join(f"[{l}]" for l in fg_lbls) + f"concat=n={n_clips}:v=1[{fg_out}]")

  # Clips dans l'ordre de lecture (data[-1] en premier = input 0)
  clips_in_order = list(reversed(data[1:] if teaser_top else data))

  anorm_lbls: list[str] = []
  for i in range(n_clips):
    lbl     = _next_lbl()
    filters = "aresample=44100,aformat=sample_fmts=fltp:channel_layouts=stereo"
    if smooth_transition and smooth_transition.get("active"):
      fade_d  = smooth_transition.get("duration", 0.3)
      dur     = clips_in_order[i]["duration"]
      is_last = (i == n_clips - 1)
      if not is_last:
        filters += f",afade=t=out:st={dur - fade_d}:d={fade_d}"
      if i > 0:
        filters += f",afade=t=in:st=0:d={fade_d}"
    parts.append(f"[{i}:a]{filters}[{lbl}]")
    anorm_lbls.append(lbl)

  parts.append("".join(f"[{l}]" for l in anorm_lbls) + f"concat=n={n_clips}:v=0:a=1[ca]")

  v0 = _next_lbl()
  parts.append(f"[{bg_out}][{fg_out}]overlay={video_margin}:{fg_y}[{v0}]")
  cur = v0

  # ── Helper partagé : header global ───────────────────────────────────────────

  def _render_global_header(cur: str) -> str:
    ts_style = title.get("titleStyle") or {}
    ss_style = title.get("subtitleStyle") or {}

    cur = _overlay_text(
      cur, title["first"],
      fontsize=_v(ts_style, "size", TITLE_FONTSIZE),
      border_w=_v(ts_style, "border", TITLE_BORDER),
      x_mode="center", y_px=TITLE1_Y,
      font=ts_style.get("font"),
      color=ts_style.get("color") or "0xFFFFFF",
      opacity=_v(ts_style, "opacity", 1.0),
    )

    if title.get("second"):
      cur = _overlay_text(
        cur, title["second"],
        fontsize=_v(ts_style, "size", TITLE_FONTSIZE),
        border_w=_v(ts_style, "border", TITLE_BORDER),
        x_mode="center", y_px=TITLE2_Y,
        font=ts_style.get("font"),
        color=ts_style.get("color") or "0xFFFFFF",
        opacity=_v(ts_style, "opacity", 1.0),
      )

    if title.get("subtitle"):
      title_size = _v(ts_style, "size", TITLE_FONTSIZE)
      subtitle_y = SUBTITLE_Y if title.get("second") else TITLE1_Y + title_size + 10
      cur = _overlay_text(
        cur, title["subtitle"],
        fontsize=_v(ss_style, "size", SUBTITLE_FONTSIZE),
        border_w=_v(ss_style, "border", SUBTITLE_BORDER),
        x_mode="center", y_px=subtitle_y,
        font=ss_style.get("font"),
        color=ss_style.get("color") or "0xFFFFFF",
        opacity=_v(ss_style, "opacity", 1.0),
      )

    return cur

  # ── Template : top ────────────────────────────────────────────────────────────

  def build_top_filter(cur: str) -> str:
    cur = _render_global_header(cur)

    n                 = len(data)
    hl                = highlight or {}
    hl_active         = hl.get("active", False)
    hl_inactive_color = hl.get("inactiveColor", "0x888888")

    if teaser_top and n > 1:
      real_data    = data[1:]
      t_total_real = sum(item["duration"] for item in real_data)
      t_teaser     = t_total_real - data[0]["duration"]
    else:
      real_data = data
      t_teaser  = None

    real_n = len(real_data)
    offset = 1 if teaser_top else 0

    # Timestamps d'apparition par index dans real_data
    appearance: dict[int, float] = {}
    t_acc = 0.0
    for idx_pb in range(real_n - 1, -1, -1):
      appearance[idx_pb] = t_acc
      t_acc += real_data[idx_pb]["duration"]

    list_top    = SUBTITLE_Y + SUBTITLE_FONTSIZE + 20
    WIN         = WINDOW_SIZE
    use_sliding = n > WIN
    n_slides    = n - WIN if use_sliding else 0
    slot_h      = (fg_y - list_top) / (WIN if use_sliding else n)

    # ── Slide times ────────────────────────────────────────────────────────────
    # Slide fires when the active clip reaches position 2 from the top.
    # = at the start of real_data[real_n-4-k]'s clip (WIN-2 clips play first).
    slide_times: list[float] = []
    if use_sliding:
      for k in range(n_slides):
        j = real_n - 4 - k
        if j >= 0:
          slide_times.append(appearance[j])

    # ── Helpers sliding window ────────────────────────────────────────────────

    def _si_sf(i: int) -> tuple[int, int]:
      """First and last states (window positions) where item i is visible."""
      return max(0, n - WIN - i), min(n_slides, n - 1 - i)

    def _relevant_slides(si: int, sf: int) -> list[float]:
      """Slide timestamps this item participates in (entry + internal + exit)."""
      first = (si - 1) if si > 0 else 0
      last  = sf if sf < n_slides else sf - 1
      return slide_times[first:last + 1] if first <= last else []

    def _k_initial(i: int, si: int) -> float:
      """Slot index just before the first relevant slide (−1 = one above window for entering items)."""
      base = float(i - n + WIN + si)
      return base - 1.0 if si > 0 else base  # si>0: enters from one slot above

    def _y_expr_sliding(i: int, extra: float = 0.0, canvas: bool = False, item_h: float | None = None) -> str:
      """Y ffmpeg expression for item i.
      canvas=True: Y relative to list_top (for canvas-based clipping)."""
      si, sf   = _si_sf(i)
      rel      = _relevant_slides(si, sf)
      k_init   = _k_initial(i, si)
      h        = item_h if item_h is not None else RANK_FONTSIZE
      row_off  = (slot_h - h) / 2 + extra
      base     = 0.0 if canvas else list_top

      def y_at(k: float) -> float:
        return base + k * slot_h + row_off

      final_k = k_init + len(rel)
      expr    = f"{y_at(final_k):.2f}"

      for idx in range(len(rel) - 1, -1, -1):
        T   = rel[idx]
        k_s = k_init + idx
        y_s = y_at(k_s)
        sl  = f"{y_s:.2f}+(t-{T:.4f})/{SLIDE_DUR:.3f}*{slot_h:.2f}"
        expr = f"if(lt(t,{T:.4f}),{y_s:.2f},if(lt(t,{T+SLIDE_DUR:.4f}),{sl},{expr}))"

      return f"'{expr}'"

    def _win_enable(i: int) -> str:
      """Enable expression for item i's window visibility."""
      si, sf  = _si_sf(i)
      t_start = slide_times[si - 1] if si > 0 else 0.0
      if sf < n_slides:
        t_end = slide_times[sf] + SLIDE_DUR
        return f"between(t,{t_start:.4f},{t_end:.4f})"
      return f"gte(t,{t_start:.4f})" if t_start > 0 else ""

    def _win_alpha(i: int) -> str:
      """Fade-in (entry) and fade-out (exit) alpha filters for item i."""
      si, sf = _si_sf(i)
      a = ""
      if si > 0:
        a += f",fade=t=in:st={slide_times[si-1]:.4f}:d={SLIDE_DUR}:alpha=1"
      if sf < n_slides:
        a += f",fade=t=out:st={slide_times[sf]:.4f}:d={SLIDE_DUR}:alpha=1"
      return a

    def _ov_png(cur_lbl: str, text: str, font_name: str | None, fontsize: int,
                border_w: int, color: str, x_mode: str, y_str: str,
                alpha_extra: str, opacity: float, enable_expr: str) -> str:
      """Génère un PNG et l'overlay avec une expression Y dynamique."""
      fp       = _resolve_font(font_name)
      k        = _next_inp()
      pp       = os.path.join(overlay_dir, f"text_{k}.png")
      pw, _    = text_render.render_text_png(text, fp, fontsize, color, border_w, pp)
      extra_inputs.append(pp)
      x        = (OUT_W - pw) // 2 if x_mode == "center" else int(x_mode.split(":")[1])
      op       = f",colorchannelmixer=aa={opacity:.2f}" if opacity < 1.0 else ""
      lbl      = _next_lbl()
      parts.append(f"[{k}:v]format=rgba{alpha_extra}{op}[{lbl}]")
      en       = f":enable='{enable_expr}'" if enable_expr else ""
      out      = _next_lbl()
      parts.append(f"[{cur_lbl}][{lbl}]overlay=x={x}:y={y_str}{en}[{out}]")
      return out

    # ── Canvas transparent pour clipper les overlays de liste ────────────────
    # Tout le texte de la liste est rendu sur ce canvas puis overlay sur cur.
    # Les items qui slident en dehors de [list_top, fg_y] sont clippés.
    list_h   = fg_y - list_top
    cv_lbl   = _next_lbl()
    parts.append(
      f"color=c=black@0:s={OUT_W}x{list_h}:r=30,format=rgba[{cv_lbl}]"
    )
    cv = cv_lbl  # label courant du canvas

    # ── Render items ──────────────────────────────────────────────────────────

    if not use_sliding:

      # n ≤ WIN : comportement classique — Y relatif au canvas (- list_top)
      for i, item in enumerate(data):
        y_center  = int(slot_h * i + slot_h / 2)  # centre vertical du slot
        is_teaser = teaser_top and i == 0

        if is_teaser:
          ts_val, ts_end = t_teaser, None
        else:
          real_i = i - offset
          ts_val = appearance[real_i]
          ts_end = t_teaser if (teaser_top and i == 1) else ts_val + item["duration"]

        cs          = item.get("titleStyle") or {}
        ids         = item.get("idStyle") or {}
        anim        = cs.get("animation", "fade")
        pos         = cs.get("position", "left")
        has_id      = bool(str(item["id"]).strip())

        # Hauteurs réelles pour aligner ID et titre sur le même centre vertical
        id_h    = text_render.text_height(str(item["id"]), _resolve_font(ids.get("font")), _v(ids, "size", RANK_FONTSIZE), _v(ids, "border", RANK_BORDER)) if has_id else 0
        title_h = text_render.text_height(item["title"], _resolve_font(cs.get("font")), _v(cs, "size", CLIP_TITLE_FONTSIZE), _v(cs, "border", CLIP_TITLE_BORDER))
        id_y    = y_center - id_h // 2
        title_y = y_center - title_h // 2

        if pos == "center":
          title_x = "center"
        elif has_id:
          title_x = f"fixed:{CLIP_TITLE_X}"
        else:
          title_x = f"fixed:{RANK_X}"

        if has_id:
          cv = _overlay_text(
            cv, str(item["id"]),
            fontsize=_v(ids, "size", RANK_FONTSIZE),
            border_w=_v(ids, "border", RANK_BORDER),
            x_mode=f"fixed:{RANK_X}", y_px=id_y,
            font=ids.get("font"),
            color=ids.get("color") or "0xFFFFFF",
            appear_at=ts_val if hl_active else 0.0,
            animation="none",
            inactive_color=hl_inactive_color if hl_active else None,
            inactive_at=ts_end if (hl_active and ts_end is not None) else None,
            always_visible=hl_active,
            opacity=_v(ids, "opacity", 1.0),
          )

        cv = _overlay_text(
          cv, item["title"],
          fontsize=_v(cs, "size", CLIP_TITLE_FONTSIZE),
          border_w=_v(cs, "border", CLIP_TITLE_BORDER),
          x_mode=title_x, y_px=title_y,
          font=cs.get("font"),
          color=cs.get("color") or "0xFFFFFF",
          appear_at=ts_val,
          animation=anim,
          inactive_color=hl_inactive_color if hl_active else None,
          inactive_at=ts_end if (hl_active and ts_end is not None) else None,
          opacity=_v(cs, "opacity", 1.0),
        )

    else:

      # n > WIN : sliding window — Y relatif au canvas (canvas=True)
      for i, item in enumerate(data):
        is_teaser = teaser_top and i == 0

        if is_teaser:
          ts_val, ts_end = t_teaser, None
        else:
          real_i = i - offset
          ts_val = appearance.get(real_i, 0.0)
          ts_end = t_teaser if (teaser_top and i == 1) else ts_val + item["duration"]

        cs          = item.get("titleStyle") or {}
        ids         = item.get("idStyle") or {}
        pos         = cs.get("position", "left")
        has_id      = bool(str(item["id"]).strip())


        win_en  = _win_enable(i)
        alpha   = _win_alpha(i)

        id_h_val    = text_render.text_height(str(item["id"]), _resolve_font(ids.get("font")), _v(ids, "size", RANK_FONTSIZE), _v(ids, "border", RANK_BORDER)) if has_id else RANK_FONTSIZE
        title_h_val = text_render.text_height(item["title"], _resolve_font(cs.get("font")), _v(cs, "size", CLIP_TITLE_FONTSIZE), _v(cs, "border", CLIP_TITLE_BORDER))
        id_y        = _y_expr_sliding(i, canvas=True, item_h=id_h_val)
        title_y     = _y_expr_sliding(i, canvas=True, item_h=title_h_val)

        if pos == "center":
          tx = "center"
        elif has_id:
          tx = f"fixed:{CLIP_TITLE_X}"
        else:
          tx = f"fixed:{RANK_X}"

        # Animation alpha du title au moment de son apparition
        anim     = cs.get("animation", "fade")
        ts       = ts_val if ts_val is not None else 0.0
        if anim == "fade":
          anim_alpha = f",fade=t=in:st={ts:.4f}:d=0.5:alpha=1"
        elif anim in ("slide-left", "slide-bottom"):
          anim_alpha = f",fade=t=in:st={ts:.4f}:d=0.3:alpha=1"
        else:
          anim_alpha = ""

        appeared  = f"gte(t,{ts:.4f})"
        title_act = f"({win_en})*({appeared})" if win_en else appeared

        # ── ID — toujours visible dans la fenêtre ────────────────────────────
        if has_id:
          id_fs = _v(ids, "size", RANK_FONTSIZE)
          id_bw = _v(ids, "border", RANK_BORDER)
          id_op = _v(ids, "opacity", 1.0)
          id_co = ids.get("color") or "0xFFFFFF"
          id_fn = ids.get("font")
          if hl_active:
            if ts_end is not None:
              clip_period = f"between(t,{ts:.4f},{ts_end:.4f})"
            else:
              clip_period = appeared
            ina_id_en = f"({win_en})*not({clip_period})" if win_en else f"not({clip_period})"
            act_id_en = f"({win_en})*({clip_period})" if win_en else clip_period
            cv = _ov_png(cv, str(item["id"]), id_fn, id_fs, id_bw,
                         hl_inactive_color, f"fixed:{RANK_X}", id_y,
                         alpha + ",colorchannelmixer=aa=0.6", 1.0, ina_id_en)
            cv = _ov_png(cv, str(item["id"]), id_fn, id_fs, id_bw,
                         id_co, f"fixed:{RANK_X}", id_y, alpha, id_op, act_id_en)
          else:
            cv = _ov_png(cv, str(item["id"]), id_fn, id_fs, id_bw,
                         id_co, f"fixed:{RANK_X}", id_y, alpha, id_op, win_en)

        # ── Title — apparaît à ts_val, reste visible, inactive après ts_end ─
        t_fs = _v(cs, "size", CLIP_TITLE_FONTSIZE)
        t_bw = _v(cs, "border", CLIP_TITLE_BORDER)
        t_op = _v(cs, "opacity", 1.0)
        t_co = cs.get("color") or "0xFFFFFF"
        t_fn = cs.get("font")
        if hl_active and ts_end is not None:
          act_title_en = f"({win_en})*between(t,{ts:.4f},{ts_end:.4f})" if win_en else f"between(t,{ts:.4f},{ts_end:.4f})"
          ina_title_en = f"({win_en})*gte(t,{ts_end:.4f})" if win_en else f"gte(t,{ts_end:.4f})"
          cv = _ov_png(cv, item["title"], t_fn, t_fs, t_bw,
                       t_co, tx, title_y, alpha + anim_alpha, t_op, act_title_en)
          cv = _ov_png(cv, item["title"], t_fn, t_fs, t_bw,
                       hl_inactive_color, tx, title_y,
                       alpha + ",colorchannelmixer=aa=0.6", 1.0, ina_title_en)
        else:
          cv = _ov_png(cv, item["title"], t_fn, t_fs, t_bw,
                       t_co, tx, title_y, alpha + anim_alpha, t_op, title_act)

    # ── Merge canvas sur la vidéo principale ─────────────────────────────────
    merged = _next_lbl()
    parts.append(f"[{cur}][{cv}]overlay=0:{list_top}[{merged}]")
    cur = merged

    return cur

  # ── Template : classic ────────────────────────────────────────────────────────

  def build_classic_filter(cur: str) -> str:
    cur = _render_global_header(cur)

    t = 0.0
    for item in reversed(data):
      start = t
      end   = t + item["duration"]

      cs       = item.get("titleStyle") or {}
      ss       = item.get("subtitleStyle") or {}
      anim     = cs.get("animation", "fade")
      sub_anim = ss.get("animation", "fade")
      cs_size  = _v(cs, "size", CLIP_TITLE_FONTSIZE)
      sub_size = _v(ss, "size", CLIP_SUBTITLE_FONTSIZE)
      has_sub  = bool(item.get("subtitle") and str(item["subtitle"]).strip())

      pos     = cs.get("position", "center")
      title_x = "center" if pos == "center" else f"fixed:{RANK_X}"

      if has_sub:
        sub_y   = fg_y - _spacing - sub_size
        title_y = sub_y - _spacing - cs_size
      else:
        title_y = fg_y - _spacing - cs_size
        sub_y   = None

      cur = _overlay_text(
        cur, item["title"],
        fontsize=cs_size,
        border_w=_v(cs, "border", CLIP_TITLE_BORDER),
        x_mode=title_x, y_px=title_y,
        font=cs.get("font"),
        color=cs.get("color") or "0xFFFFFF",
        appear_at=start,
        animation=anim,
        active_until=end,
        opacity=_v(cs, "opacity", 1.0),
      )

      if has_sub and sub_y is not None:
        sub_pos = ss.get("position", "center")
        sub_x   = "center" if sub_pos == "center" else f"fixed:{RANK_X}"
        cur = _overlay_text(
          cur, str(item["subtitle"]),
          fontsize=sub_size,
          border_w=_v(ss, "border", CLIP_SUBTITLE_BORDER),
          x_mode=sub_x, y_px=sub_y,
          font=ss.get("font"),
          color=ss.get("color") or "0xFFFFFF",
          appear_at=start,
          animation=sub_anim,
          active_until=end,
          opacity=_v(ss, "opacity", 1.0),
        )

      t = end

    return cur

  # ── Template : minimal ────────────────────────────────────────────────────────

  def build_minimal_filter(cur: str) -> str:
    t = 0.0
    for item in reversed(data):
      start = t
      end   = t + item["duration"]

      cs       = item.get("titleStyle") or {}
      ss       = item.get("subtitleStyle") or {}
      anim     = cs.get("animation", "fade")
      sub_anim = ss.get("animation", "fade")
      cs_size  = _v(cs, "size", CLIP_TITLE_FONTSIZE)
      sub_size = _v(ss, "size", CLIP_SUBTITLE_FONTSIZE)
      has_sub  = bool(item.get("subtitle") and str(item["subtitle"]).strip())

      pos     = cs.get("position", "center")
      title_x = "center" if pos == "center" else f"fixed:{RANK_X}"

      if has_sub:
        sub_y   = fg_y - _spacing - sub_size
        title_y = sub_y - _spacing - cs_size
      else:
        title_y = fg_y - _spacing - cs_size
        sub_y   = None

      cur = _overlay_text(
        cur, item["title"],
        fontsize=cs_size,
        border_w=_v(cs, "border", CLIP_TITLE_BORDER),
        x_mode=title_x, y_px=title_y,
        font=cs.get("font"),
        color=cs.get("color") or "0xFFFFFF",
        appear_at=start,
        animation=anim,
        active_until=end,
        opacity=_v(cs, "opacity", 1.0),
      )

      if has_sub and sub_y is not None:
        sub_pos = ss.get("position", "center")
        sub_x   = "center" if sub_pos == "center" else f"fixed:{RANK_X}"
        cur = _overlay_text(
          cur, str(item["subtitle"]),
          fontsize=sub_size,
          border_w=_v(ss, "border", CLIP_SUBTITLE_BORDER),
          x_mode=sub_x, y_px=sub_y,
          font=ss.get("font"),
          color=ss.get("color") or "0xFFFFFF",
          appear_at=start,
          animation=sub_anim,
          active_until=end,
          opacity=_v(ss, "opacity", 1.0),
        )

      t = end

    return cur

  # ── Template : expanded ───────────────────────────────────────────────────────

  def build_expanded_filter(cur: str) -> str:
    # Pas de header global. Zone top = 150px, vidéo de fg_y à fg_y+fg_h, zone bottom = 150px.
    # title + subtitle (optionnel) centrés verticalement dans la zone top (0..EXPANDED_TOP).
    top_zone = EXPANDED_TOP

    t = 0.0
    for item in reversed(data):
      start = t
      end   = t + item["duration"]

      cs       = item.get("titleStyle") or {}
      ss       = item.get("subtitleStyle") or {}
      anim     = cs.get("animation", "fade")
      sub_anim = ss.get("animation", "fade")
      cs_size  = _v(cs, "size", CLIP_TITLE_FONTSIZE)
      sub_size = _v(ss, "size", CLIP_SUBTITLE_FONTSIZE)
      has_sub  = bool(item.get("subtitle") and str(item["subtitle"]).strip())

      pos     = cs.get("position", "center")
      title_x = "center" if pos == "center" else f"fixed:{RANK_X}"

      # Centrage vertical dans la zone top
      if has_sub:
        block_h   = cs_size + _spacing + sub_size
        block_top = (top_zone - block_h) // 2
        title_y   = block_top
        sub_y     = block_top + cs_size + _spacing
      else:
        title_y = (top_zone - cs_size) // 2
        sub_y   = None

      cur = _overlay_text(
        cur, item["title"],
        fontsize=cs_size,
        border_w=_v(cs, "border", CLIP_TITLE_BORDER),
        x_mode=title_x, y_px=title_y,
        font=cs.get("font"),
        color=cs.get("color") or "0xFFFFFF",
        appear_at=start,
        animation=anim,
        active_until=end,
        opacity=_v(cs, "opacity", 1.0),
      )

      if has_sub and sub_y is not None:
        sub_pos = ss.get("position", "center")
        sub_x   = "center" if sub_pos == "center" else f"fixed:{RANK_X}"
        cur = _overlay_text(
          cur, str(item["subtitle"]),
          fontsize=sub_size,
          border_w=_v(ss, "border", CLIP_SUBTITLE_BORDER),
          x_mode=sub_x, y_px=sub_y,
          font=ss.get("font"),
          color=ss.get("color") or "0xFFFFFF",
          appear_at=start,
          animation=sub_anim,
          active_until=end,
          opacity=_v(ss, "opacity", 1.0),
        )

      t = end

    return cur

  # ── Branchement sur le template ───────────────────────────────────────────────
  if template == "top":
    cur = build_top_filter(cur)
  elif template == "classic":
    cur = build_classic_filter(cur)
  elif template == "minimal":
    cur = build_minimal_filter(cur)
  elif template == "expanded":
    cur = build_expanded_filter(cur)

  # ── Watermark — positionné par rapport au bas de la vidéo ─────────────────────
  if watermark and watermark.get("active") and watermark.get("text"):
    wm_size    = watermark.get("size", 36)
    wm_opacity = max(0.0, min(1.0, float(watermark.get("opacity", 1.0))))
    cur = _overlay_text(
      cur, watermark["text"],
      fontsize=wm_size,
      border_w=0,
      x_mode=f"fixed:{video_margin + 30}",
      y_px=fg_y + fg_h - wm_size - 20,
      font=watermark.get("font"),
      color=watermark.get("color") or "0xFFFFFF",
      opacity=wm_opacity,
    )

  # ── Label de sortie [vout] ────────────────────────────────────────────────────
  pts_lbl = _next_lbl()
  parts.append(f"[{cur}]setpts=PTS-STARTPTS,fps=30[{pts_lbl}]")
  fc = ";".join(parts)
  fc = fc.replace(f"[{pts_lbl}]", "[vout]", 1)
  return fc, extra_inputs


async def render_video(job_id: str, user_id: str, payload: dict) -> None:
  """Télécharge, découpe, concatène les clips et assemble la vidéo finale."""
  from ..config import STORAGE_ROOT
  from . import yt_dlp

  data       = payload["data"]
  title      = payload["title"]
  template   = payload.get("template", "top")
  teaser_top = payload.get("teaserTop", False) and template == "top" and len(data) > 1

  clips_dir = os.path.join(STORAGE_ROOT, user_id, job_id, "clips")
  out_path  = final_path(user_id, job_id)

  os.makedirs(clips_dir, exist_ok=True)

  # ── 1. Téléchargement + extraction ───────────────────────────────────────────
  clips_status = [
    {"id": item["id"], "title": item["title"], "status": "pending"}
    for item in data
  ]
  update_job(job_id, clips=clips_status)

  clip_paths: list[str] = []

  for i, item in enumerate(data):
    # teaserTop : data[0] n'a pas de vidéo — on skip son téléchargement
    if teaser_top and i == 0:
      clips_status[0]["status"] = "done"
      update_job(job_id, clips=clips_status)
      continue

    raw_subdir = os.path.join(STORAGE_ROOT, user_id, job_id, "raw", str(i))
    os.makedirs(raw_subdir, exist_ok=True)

    clips_status[i]["status"] = "downloading"
    update_job(job_id, message=f"Téléchargement {i + 1}/{len(data)} — {item['title']}...", clips=clips_status)
    start_time = item.get("start_time") if not item.get("claude") else None
    source_file, sections_used = await yt_dlp.download(
      job_id, item["url"], raw_subdir,
      start_time=start_time,
      duration=item.get("duration"),
    )

    if item.get("claude"):
      raise NotImplementedError(
        "Détermination automatique du timestamp via Claude non encore implémentée."
      )

    # Si --download-sections a réussi, les timestamps du fichier commencent à 0
    extract_start = "00:00:00" if sections_used else start_time
    clip_path = os.path.join(clips_dir, f"clip_{i}.mp4")
    await extract_clip(job_id, source_file, extract_start, item["duration"], clip_path)

    if not os.path.exists(clip_path) or os.path.getsize(clip_path) < 1000:
      raise RuntimeError(
        f"Clip {i + 1} vide — le timestamp {start_time} "
        "dépasse probablement la durée de la vidéo source."
      )

    clips_status[i]["status"] = "done"
    update_job(job_id, clips=clips_status)
    clip_paths.append(clip_path)

  # ── 2. Assemblage final (concat filter + overlays en une seule passe) ────────
  update_job(job_id, status=PROCESSING, message="Assemblage final...")

  overlay_dir = os.path.join(STORAGE_ROOT, user_id, job_id, "overlays")
  os.makedirs(overlay_dir, exist_ok=True)

  # highlightActive et teaserTop sont exclusifs au template "top"
  highlight         = payload.get("highlightActive") if template == "top" else None
  smooth_transition = payload.get("smoothTransition")
  background        = payload.get("background", "video")
  watermark         = payload.get("watermark")

  raw_margin = payload.get("videoMargin") or 0
  video_margin = raw_margin if template in ("top", "classic", "minimal") else 0

  fc, extra_inputs = build_filter_complex(
    data, title, template, overlay_dir,
    highlight, teaser_top, smooth_transition, background, watermark,
    spacing=payload.get("spacing"),
    video_margin=video_margin,
  )

  if teaser_top:
    total_duration = sum(item["duration"] for item in data[1:])
  else:
    total_duration = sum(item["duration"] for item in data)

  cmd: list[str] = []
  for p in reversed(clip_paths):
    cmd.extend(["-i", p])
  for png_path in extra_inputs:
    cmd.extend(["-loop", "1", "-t", str(total_duration), "-i", png_path])
  cmd.extend([
    "-filter_complex", fc,
    "-map", "[vout]",
    "-map", "[ca]",
    "-c:v", "libx264",
    "-preset", "fast",
    "-crf", "18",
    "-c:a", "aac",
    "-b:a", "192k",
    "-max_muxing_queue_size", "9999",
    "-t", str(total_duration),
    out_path,
  ])

  await _run(job_id, *cmd, label="render_final")
  cleanup_job_artifacts(user_id, job_id)
