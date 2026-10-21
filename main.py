"""
Fetchbox: a small hosted downloader for YouTube links.

FastAPI + yt-dlp + FFmpeg. One process, no database, no accounts, and no
stored user data. Every job lives in its own temp folder that is removed as
soon as the response has been sent (or as soon as anything fails).

All YouTube traffic goes through the residential proxy in YTDLP_PROXY,
because YouTube blocks most datacenter IP ranges (Render included).
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import secrets
import shutil
import tempfile
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from ipaddress import ip_address, ip_network
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from starlette.background import BackgroundTask
from starlette.datastructures import MutableHeaders
from starlette.middleware.cors import CORSMiddleware
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadCancelled
from yt_dlp.version import __version__ as YTDLP_VERSION

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("fetchbox")


# --------------------------------------------------------------------------
# Configuration (all via environment variables)
# --------------------------------------------------------------------------
def _int_env(name: str, default: int, minimum: int = 0) -> int:
    try:
        value = int(os.environ.get(name, "").strip() or default)
    except ValueError:
        value = default
    return max(value, minimum)


BASE_DIR = Path(__file__).resolve().parent
INDEX_HTML = BASE_DIR / "index.html"

# Proxy, e.g. http://user:pass@host:port. May contain the placeholder
# {session}, which is replaced by a fresh random id for every job so that a
# provider's "sticky session" feature keeps one job on one IP address.
_proxy_raw = os.environ.get("YTDLP_PROXY", "").strip()
_PROXY_SCHEMES = ("http://", "https://", "socks4://", "socks5://", "socks5h://")
PROXY_TEMPLATE = _proxy_raw if _proxy_raw.lower().startswith(_PROXY_SCHEMES) else ""
PROXY_INVALID = bool(_proxy_raw) and not PROXY_TEMPLATE

# Render mounts secret files read-only under /etc/secrets.
COOKIES_SRC = Path(os.environ.get("COOKIES_FILE", "/etc/secrets/cookies.txt"))

MAX_DURATION_MIN = _int_env("MAX_DURATION_MIN", 60, 1)
RATE_LIMIT_DOWNLOADS = _int_env("RATE_LIMIT_DOWNLOADS", 5, 1)  # per IP per window
RATE_LIMIT_INFO = _int_env("RATE_LIMIT_INFO", 30, 1)  # lookups per IP per window
RATE_LIMIT_WINDOW_MIN = _int_env("RATE_LIMIT_WINDOW_MIN", 10, 1)
GLOBAL_DOWNLOADS_PER_HOUR = _int_env("GLOBAL_DOWNLOADS_PER_HOUR", 100, 0)  # 0 = off
MAX_CONCURRENT_JOBS = _int_env("MAX_CONCURRENT_JOBS", 2, 1)
MAX_CONCURRENT_LOOKUPS = _int_env("MAX_CONCURRENT_LOOKUPS", 4, 1)
QUEUE_WAIT_SEC = _int_env("QUEUE_WAIT_SEC", 20, 1)
JOB_TIMEOUT_SEC = _int_env("JOB_TIMEOUT_SEC", 900, 30)
# How many reverse proxies in front of the app append to X-Forwarded-For.
# The client address is taken that many entries from the right (spoof-safe).
TRUSTED_PROXY_HOPS = _int_env("TRUSTED_PROXY_HOPS", 1, 0)
DEBUG_ERRORS = os.environ.get("DEBUG_LOG_ERRORS", "").strip() == "1"

TMP_ROOT = Path(os.environ.get("TMP_ROOT", "") or tempfile.gettempdir()) / "fetchbox-jobs"
JOB_TTL_SEC = 2 * 60 * 60  # safety net: orphaned job folders older than this are purged
FFMPEG_PATH = shutil.which("ffmpeg")


# --------------------------------------------------------------------------
# Errors: every failure becomes {"error": {code, message, hint, retry_after}}
# --------------------------------------------------------------------------
ERRORS: dict[str, tuple[int, str, str]] = {
    "unsupported_link": (
        400,
        "That isn't a link this tool can use.",
        "Paste a link to a single video from youtube.com, music.youtube.com or youtu.be.",
    ),
    "playlist_link": (
        400,
        "That link points to a playlist, not a single video.",
        "Open the video you want and paste its own link.",
    ),
    "bad_request": (
        400,
        "Those download options aren't valid.",
        "Reload the page and choose a format again.",
    ),
    "rate_limited": (
        429,
        "You've reached the limit of {limit} downloads per {minutes} minutes.",
        "Try again in about {wait}.",
    ),
    "info_rate_limited": (
        429,
        "Too many lookups in a short time.",
        "Wait about {wait} and try again.",
    ),
    "global_limited": (
        429,
        "This service has reached its hourly download limit.",
        "Try again in about {wait}.",
    ),
    "busy": (
        503,
        "The server is busy preparing other files.",
        "Try again in a minute.",
    ),
    "too_long": (
        413,
        "This video is {duration} long, and the limit is {limit} minutes.",
        "Choose a shorter video. The limit keeps proxy data use under control.",
    ),
    "live": (
        422,
        "This is a live stream or an upcoming premiere.",
        "Try again after the stream has ended and YouTube has finished processing it.",
    ),
    "private": (
        403,
        "This video is private.",
        "Private videos can't be downloaded. Ask the owner to make it public or unlisted.",
    ),
    "age_restricted": (
        403,
        "This video is age-restricted.",
        "YouTube only shows it to signed-in adults. The site owner can add a cookies.txt file from a signed-in account (see the README).",
    ),
    "members_only": (
        403,
        "This video is for channel members only.",
        "Only public and unlisted videos can be downloaded.",
    ),
    "geo_blocked": (
        403,
        "YouTube says this video isn't available in the server's region.",
        "The site owner can choose a proxy located in another country.",
    ),
    "unavailable": (
        404,
        "This video is unavailable.",
        "It may have been removed or made private. Check that the link opens in your browser.",
    ),
    "proxy_missing": (
        503,
        "YouTube blocked this server, and no residential proxy is set up.",
        "If you run this site, set YTDLP_PROXY and redeploy (see the README). Visitors: please tell the site owner.",
    ),
    "blocked_ip": (
        502,
        "YouTube refused the server's connection (it asked to confirm this isn't a bot).",
        "Try again in a minute; each attempt uses a fresh connection. If it keeps happening, the site owner should check the proxy or add cookies.txt.",
    ),
    "proxy_failure": (
        502,
        "The server couldn't connect through its proxy.",
        "Try again shortly. If it persists, the site owner should check the proxy address, login and data balance.",
    ),
    "ffmpeg_missing": (
        500,
        "FFmpeg isn't available on the server, so files can't be converted.",
        "The site owner needs to install FFmpeg (the included Dockerfile does).",
    ),
    "format_unavailable": (
        422,
        "That format or quality isn't available for this video.",
        "Choose a lower quality or a different format.",
    ),
    "timeout": (
        504,
        "Preparing the file took too long and was stopped.",
        "Try a lower quality or a shorter video.",
    ),
    "network": (
        502,
        "The connection to YouTube dropped while preparing your file.",
        "Try again.",
    ),
    "upstream_limited": (
        502,
        "YouTube is asking this server to slow down.",
        "Wait a minute and try again.",
    ),
    "failed": (
        502,
        "Something went wrong while preparing your file.",
        "Try again. If it keeps failing, the site owner may need to update yt-dlp (see the README).",
    ),
}
TRANSIENT_CODES = {"blocked_ip", "proxy_failure", "network", "upstream_limited"}


class AppError(Exception):
    def __init__(
        self,
        code: str,
        status: int,
        message: str,
        hint: str = "",
        retry_after: int | None = None,
        transient: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
        self.message = message
        self.hint = hint
        self.retry_after = retry_after
        self.transient = transient


def make(code: str, *, retry_after: int | None = None, **fmt: Any) -> AppError:
    status, message, hint = ERRORS[code]
    return AppError(
        code,
        status,
        message.format(**fmt),
        hint.format(**fmt),
        retry_after,
        transient=code in TRANSIENT_CODES,
    )


def wait_text(seconds: int) -> str:
    seconds = max(1, int(seconds))
    if seconds < 90:
        return f"{seconds} seconds"
    return f"{-(-seconds // 60)} minutes"


def duration_text(seconds: int) -> str:
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_TRANSIENT_HINTS = (
    "timed out", "timeout", "connection reset", "connection aborted",
    "remote end closed", "incompleteread", "temporary failure",
    "name resolution", "http error 5", "bad gateway", "service unavailable",
    "gateway time-out", "unable to download", "eof occurred", "transporterror",
    "read error", "network is unreachable", "connection refused",
)


def classify(exc: BaseException) -> AppError:
    """Turn any exception from yt-dlp into a friendly AppError."""
    if isinstance(exc, AppError):
        return exc
    if isinstance(exc, DownloadCancelled):
        return make("timeout")

    low = _ANSI.sub("", str(exc)).lower()

    def has(*needles: str) -> bool:
        return any(n in low for n in needles)

    if has("ffmpeg", "ffprobe") and has("not found", "not installed", "no such file", "install"):
        return make("ffmpeg_missing")
    if has("proxyerror", "unable to connect to proxy", "proxy authentication",
           "tunnel connection failed", "407 proxy"):
        return make("proxy_failure")
    # Age checks must come before the generic "sign in to confirm" bot check.
    if has("confirm your age", "age-restricted", "age restricted", "inappropriate for some users"):
        return make("age_restricted")
    if has("private video", "this video is private"):
        return make("private")
    if has("members-only", "members only", "join this channel"):
        return make("members_only")
    if has("not available in your country", "blocked it in your country",
           "blocked in your country", "who has blocked it"):
        return make("geo_blocked")
    if has("live event", "premieres in", "this live"):
        return make("live")
    if has("not a bot", "sign in to confirm", "http error 403"):
        return make("blocked_ip") if PROXY_TEMPLATE else make("proxy_missing")
    if has("requested format is not available"):
        return make("format_unavailable")
    if has("video unavailable", "not available", "has been removed", "terminated",
           "no longer available", "does not exist", "copyright"):
        return make("unavailable")
    if has("http error 429", "too many requests"):
        return make("upstream_limited")
    if has(*_TRANSIENT_HINTS):
        return make("network")

    if DEBUG_ERRORS:  # off by default: messages can contain video ids or hosts
        log.warning("unclassified yt-dlp error: %s", low[:400])
    return make("failed")


# --------------------------------------------------------------------------
# Link validation: only single YouTube videos, rebuilt into a canonical URL
# --------------------------------------------------------------------------
_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
_YT_HOSTS = {"youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com"}
_PATH_ID_RE = re.compile(r"^/(?:shorts|live|embed|v)/([^/?#]+)")


def extract_video_id(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw or len(raw) > 2048:
        raise make("unsupported_link")
    if "://" not in raw:
        raw = "https://" + raw
    try:
        parsed = urlparse(raw)
        host = (parsed.hostname or "").lower()
    except ValueError:
        raise make("unsupported_link") from None
    if parsed.scheme not in ("http", "https"):
        raise make("unsupported_link")

    video_id: str | None = None
    if host == "youtu.be":
        video_id = parsed.path.lstrip("/").split("/")[0]
    elif host in _YT_HOSTS:
        if parsed.path == "/watch":
            video_id = (parse_qs(parsed.query).get("v") or [None])[0]
        else:
            m = _PATH_ID_RE.match(parsed.path)
            video_id = m.group(1) if m else None
            if not video_id and parsed.path == "/playlist":
                raise make("playlist_link")
    else:
        raise make("unsupported_link")

    if not video_id or not _ID_RE.match(video_id):
        if host in _YT_HOSTS and "list" in parse_qs(parsed.query):
            raise make("playlist_link")
        raise make("unsupported_link")
    return video_id  # any ?list= parameter is simply ignored


# --------------------------------------------------------------------------
# Requested format / quality
# --------------------------------------------------------------------------
AUDIO_FORMATS = {"mp3", "m4a", "opus", "wav"}
VIDEO_FORMATS = {"mp4", "webm"}
MP3_BITRATES = {128, 192, 320}
MEDIA_TYPES = {
    "mp3": "audio/mpeg", "m4a": "audio/mp4", "opus": "audio/ogg",
    "wav": "audio/wav", "mp4": "video/mp4", "webm": "video/webm",
}


@dataclass(frozen=True)
class Spec:
    fmt: str
    bitrate: int | None = None  # mp3 only
    height: int | None = None  # video only; None = best available


def parse_spec(fmt: str, quality: str) -> Spec:
    fmt = (fmt or "").strip().lower()
    quality = (quality or "").strip().lower()
    if fmt in AUDIO_FORMATS:
        bitrate = None
        if fmt == "mp3":
            if quality in ("", "best"):
                bitrate = 192
            elif quality.isascii() and quality.isdigit():
                bitrate = int(quality)
            if bitrate not in MP3_BITRATES:
                raise make("bad_request")
        return Spec(fmt, bitrate=bitrate)
    if fmt in VIDEO_FORMATS:
        height = None
        if quality not in ("", "best"):
            if not re.fullmatch(r"\d{3,4}", quality):
                raise make("bad_request")
            height = int(quality)
            if not 144 <= height <= 4320:
                raise make("bad_request")
        return Spec(fmt, height=height)
    raise make("bad_request")


def format_options(spec: Spec) -> dict[str, Any]:
    """yt-dlp format selector and post-processing for the requested output."""
    h = f"[height<={spec.height}]" if spec.height else ""
    if spec.fmt == "mp3":
        return {
            "format": "bestaudio/best",
            "postprocessors": [
                {"key": "FFmpegExtractAudio", "preferredcodec": "mp3",
                 "preferredquality": str(spec.bitrate)},
                {"key": "FFmpegMetadata", "add_metadata": True},
            ],
        }
    if spec.fmt == "m4a":
        return {
            "format": "bestaudio[ext=m4a]/bestaudio/best",
            "postprocessors": [
                {"key": "FFmpegExtractAudio", "preferredcodec": "m4a"},
                {"key": "FFmpegMetadata", "add_metadata": True},
            ],
        }
    if spec.fmt == "opus":
        return {
            "format": "bestaudio[acodec=opus]/bestaudio/best",
            "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "opus"}],
        }
    if spec.fmt == "wav":
        return {
            "format": "bestaudio/best",
            "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "wav"}],
        }
    if spec.fmt == "mp4":  # H.264 + AAC first; newer codecs only when nothing else exists
        return {
            "format": (
                f"bestvideo{h}[vcodec^=avc1]+bestaudio[ext=m4a]"
                f"/bestvideo{h}[ext=mp4]+bestaudio[ext=m4a]"
                f"/bestvideo{h}+bestaudio/best{h}"
            ),
            "merge_output_format": "mp4",
        }
    return {  # webm
        "format": (
            f"bestvideo{h}[ext=webm]+bestaudio[ext=webm]"
            f"/best{h}[ext=webm]/bestvideo{h}+bestaudio/best{h}"
        ),
        "merge_output_format": "webm",
    }


# --------------------------------------------------------------------------
# Rate limiting (in memory only)
# --------------------------------------------------------------------------
class SlidingWindowLimiter:
    def __init__(self, limit: int, window_sec: int) -> None:
        self.limit = limit
        self.window = window_sec
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def acquire(self, key: str) -> tuple[bool, int]:
        """Record a hit. Returns (allowed, seconds_until_next_slot)."""
        if len(self._hits) > 20_000:
            self.sweep()
        now = time.monotonic()
        q = self._hits[key]
        while q and now - q[0] >= self.window:
            q.popleft()
        if len(q) >= self.limit:
            return False, max(1, int(self.window - (now - q[0])) + 1)
        q.append(now)
        return True, 0

    def refund(self, key: str) -> None:
        q = self._hits.get(key)
        if q:
            q.pop()

    def sweep(self) -> None:
        now = time.monotonic()
        for key in list(self._hits):
            q = self._hits[key]
            while q and now - q[0] >= self.window:
                q.popleft()
            if not q:
                del self._hits[key]


_WINDOW_SEC = RATE_LIMIT_WINDOW_MIN * 60
dl_limiter = SlidingWindowLimiter(RATE_LIMIT_DOWNLOADS, _WINDOW_SEC)
info_limiter = SlidingWindowLimiter(RATE_LIMIT_INFO, _WINDOW_SEC)
global_limiter = SlidingWindowLimiter(max(GLOBAL_DOWNLOADS_PER_HOUR, 1), 3600)


def client_ip(request: Request) -> str:
    parts = [p.strip() for p in request.headers.get("x-forwarded-for", "").split(",") if p.strip()]
    candidate = None
    if TRUSTED_PROXY_HOPS and len(parts) >= TRUSTED_PROXY_HOPS:
        candidate = parts[-TRUSTED_PROXY_HOPS]
    if not candidate:
        candidate = request.client.host if request.client else "unknown"
    try:
        addr = ip_address(candidate)
    except ValueError:
        return "unknown"
    if addr.version == 6:  # one household/host usually owns a whole /64
        return str(ip_network(f"{addr}/64", strict=False))
    return str(addr)


# --------------------------------------------------------------------------
# Temp folders
# --------------------------------------------------------------------------
def remove_dir(path: Path | str | None) -> None:
    if path:
        shutil.rmtree(path, ignore_errors=True)


def new_job_dir() -> Path:
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    job = Path(tempfile.mkdtemp(prefix="job_", dir=TMP_ROOT))  # mode 0700
    (job / "work").mkdir()
    return job


def purge_stale_jobs(max_age_sec: int | None) -> None:
    """Delete job folders (all of them when max_age_sec is None)."""
    if not TMP_ROOT.exists():
        return
    cutoff = None if max_age_sec is None else time.time() - max_age_sec
    for p in TMP_ROOT.glob("job_*"):
        with contextlib.suppress(OSError):
            if cutoff is None or p.stat().st_mtime < cutoff:
                remove_dir(p)


def cookies_readable() -> bool:
    return COOKIES_SRC.is_file() and os.access(COOKIES_SRC, os.R_OK)


def prepare_cookies(job_dir: Path) -> str | None:
    """Render secret files are read-only, but yt-dlp wants to write its jar."""
    if not cookies_readable():
        return None
    try:
        dst = job_dir / "cookies.txt"
        shutil.copyfile(COOKIES_SRC, dst)
        os.chmod(dst, 0o600)
        return str(dst)
    except OSError:
        log.warning("Could not copy the cookies file; continuing without it.")
        return None


# --------------------------------------------------------------------------
# yt-dlp jobs (these run in worker threads)
# --------------------------------------------------------------------------
class _QuietLogger:
    def debug(self, msg: str) -> None: ...
    def info(self, msg: str) -> None: ...
    def warning(self, msg: str) -> None: ...
    def error(self, msg: str) -> None: ...


def _deadline_hook(deadline: float) -> Callable[[dict], None]:
    def hook(_: dict) -> None:
        if time.monotonic() > deadline:
            raise DownloadCancelled("deadline reached")
    return hook


def base_options(session: str, cookiefile: str | None, deadline: float) -> dict[str, Any]:
    opts: dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "no_color": True,
        "logger": _QuietLogger(),
        "noplaylist": True,
        "cachedir": False,  # never write yt-dlp's cache to disk
        "socket_timeout": 20,
        "retries": 3,
        "fragment_retries": 3,
        "extractor_retries": 2,
        "concurrent_fragment_downloads": 4,
        "overwrites": True,
        "restrictfilenames": True,
        "updatetime": False,
        "progress_hooks": [_deadline_hook(deadline)],
    }
    if PROXY_TEMPLATE:
        # One session id per attempt keeps a job on one exit IP (YouTube media
        # URLs are bound to the IP that requested them) while a retry gets a new IP.
        opts["proxy"] = PROXY_TEMPLATE.replace("{session}", session)
    if cookiefile:
        opts["cookiefile"] = cookiefile
    return opts


def check_info(info: dict | None) -> dict:
    if not info:
        raise make("failed")
    if info.get("is_live") or info.get("live_status") in ("is_live", "is_upcoming", "post_live"):
        raise make("live")
    duration = info.get("duration")
    if duration and duration > MAX_DURATION_MIN * 60:
        raise make("too_long", duration=duration_text(duration), limit=MAX_DURATION_MIN)
    return info


def _retrying(attempt_fn: Callable[[str], Any], on_retry: Callable[[], None] | None = None) -> Any:
    """Run attempt_fn(session_id); retry once through the proxy on transient errors."""
    for attempt in (1, 2):
        try:
            return attempt_fn(secrets.token_hex(6))
        except Exception as exc:  # noqa: BLE001
            err = classify(exc)
            if attempt == 1 and err.transient:
                log.info("transient failure (%s); retrying once", err.code)
                if on_retry:
                    on_retry()
                continue
            raise err from None
    raise make("failed")


def summarize(info: dict, video_id: str) -> dict[str, Any]:
    video_bytes: dict[int, int] = {}
    audio_best = 0
    for f in info.get("formats") or []:
        if f.get("has_drm"):
            continue
        size = int(f.get("filesize") or f.get("filesize_approx") or 0)
        vcodec, acodec, height = f.get("vcodec"), f.get("acodec"), f.get("height")
        if vcodec and vcodec != "none" and height:
            video_bytes[int(height)] = max(video_bytes.get(int(height), 0), size)
        elif acodec and acodec != "none":
            audio_best = max(audio_best, size)
    heights = sorted(video_bytes, reverse=True)
    sizes = {str(h): (video_bytes[h] + audio_best) if video_bytes[h] else None for h in heights}
    duration = info.get("duration")
    return {
        "id": video_id,
        "title": info.get("title") or "Untitled",
        "uploader": info.get("uploader") or info.get("channel") or "",
        "duration": int(duration) if duration else None,
        "thumbnail": f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg",
        "heights": heights,
        "sizes": sizes,
        "audio_bytes": audio_best or None,
        "max_duration_min": MAX_DURATION_MIN,
    }


def run_info(video_id: str) -> dict[str, Any]:
    url = f"https://www.youtube.com/watch?v={video_id}"
    job = new_job_dir()  # only holds the per-job cookie copy
    try:
        cookies = prepare_cookies(job)

        def attempt(session: str) -> dict[str, Any]:
            opts = base_options(session, cookies, time.monotonic() + 120)
            with YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False, process=False)
                return summarize(check_info(info), video_id)

        return _retrying(attempt)
    finally:
        remove_dir(job)


@dataclass
class DownloadResult:
    path: Path
    title: str
    ext: str


def _clear_dir(path: Path) -> None:
    for child in path.iterdir():
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            with contextlib.suppress(OSError):
                child.unlink()


def _find_output(result: dict | None, work: Path, ext: str) -> Path | None:
    for rd in (result or {}).get("requested_downloads") or []:
        p = rd.get("filepath")
        if p and Path(p).is_file():
            return Path(p)
    files = [
        p for p in work.iterdir()
        if p.is_file() and p.suffix.lower() not in {".part", ".ytdl", ".temp", ".json"}
    ]
    pool = [p for p in files if p.suffix.lower() == f".{ext}"] or files
    return max(pool, key=lambda p: p.stat().st_size) if pool else None


def run_download(video_id: str, spec: Spec, job_dir: Path) -> DownloadResult:
    url = f"https://www.youtube.com/watch?v={video_id}"
    work = job_dir / "work"
    deadline = time.monotonic() + JOB_TIMEOUT_SEC
    cookies = prepare_cookies(job_dir)

    def attempt(session: str) -> DownloadResult:
        opts = base_options(session, cookies, deadline)
        opts.update(format_options(spec))
        opts["outtmpl"] = str(work / "%(id)s.%(ext)s")
        with YoutubeDL(opts) as ydl:
            # One extraction serves both the checks and the download.
            info = check_info(ydl.extract_info(url, download=False, process=False))
            result = ydl.process_ie_result(info, download=True)
        out = _find_output(result, work, spec.fmt)
        if not out:
            raise make("failed")
        title = (result or info).get("title") or video_id
        return DownloadResult(out, title, out.suffix.lstrip(".") or spec.fmt)

    try:
        return _retrying(attempt, on_retry=lambda: _clear_dir(work))
    except BaseException:
        remove_dir(job_dir)  # failed jobs never leave files behind
        raise
    finally:
        if cookies:  # the cookie copy is removed as soon as yt-dlp is done
            with contextlib.suppress(OSError):
                Path(cookies).unlink()


# --------------------------------------------------------------------------
# Concurrency helpers
# --------------------------------------------------------------------------
job_sem = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
info_sem = asyncio.Semaphore(MAX_CONCURRENT_LOOKUPS)


async def acquire_slot(sem: asyncio.Semaphore) -> None:
    try:
        await asyncio.wait_for(sem.acquire(), timeout=QUEUE_WAIT_SEC)
    except asyncio.TimeoutError:
        raise make("busy") from None


def spawn(sem: asyncio.Semaphore, fn: Callable[[], Any]) -> asyncio.Future:
    """Run fn in a thread. The slot is released only when the thread really ends,
    even if the client disconnects and the request is cancelled."""
    fut = asyncio.ensure_future(asyncio.to_thread(fn))

    def _done(f: asyncio.Future) -> None:
        sem.release()
        if not f.cancelled():
            f.exception()  # mark as retrieved

    fut.add_done_callback(_done)
    return fut


# --------------------------------------------------------------------------
# App
# --------------------------------------------------------------------------
SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "X-Frame-Options": "DENY",
    "Content-Security-Policy": (
        "default-src 'self'; img-src 'self' https://i.ytimg.com data:; "
        "style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; "
        "connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
    ),
}


class SecurityHeaders:
    """Pure ASGI middleware, so it never interferes with streaming responses."""

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message: dict) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                for key, value in SECURITY_HEADERS.items():
                    headers.setdefault(key, value)
            await send(message)

        await self.app(scope, receive, send_with_headers)


async def _janitor() -> None:
    while True:
        await asyncio.sleep(300)
        purge_stale_jobs(JOB_TTL_SEC)
        dl_limiter.sweep()
        info_limiter.sweep()
        global_limiter.sweep()


def startup_self_check() -> None:
    if PROXY_INVALID:
        log.warning("YTDLP_PROXY is set but doesn't start with http://, https://, socks5:// or similar. It is being ignored.")
    elif not PROXY_TEMPLATE:
        log.warning("YTDLP_PROXY is not set. YouTube usually blocks datacenter IPs, so downloads will likely fail.")
    if not FFMPEG_PATH:
        log.error("FFmpeg was not found on PATH. Conversions and merges will fail.")
    if not shutil.which("deno"):
        log.warning("deno was not found on PATH. yt-dlp may not be able to solve YouTube's JavaScript challenges.")
    if COOKIES_SRC.exists() and not cookies_readable():
        log.warning("A cookies file exists but isn't readable by this user.")


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI):
    purge_stale_jobs(None)
    startup_self_check()
    task = asyncio.create_task(_janitor())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        purge_stale_jobs(None)


app = FastAPI(
    title="Fetchbox",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=lifespan,
)
app.add_middleware(SecurityHeaders)

# Needed when the page is hosted on a different origin than this backend.
# The Vercel frontend is allowed by default; ALLOWED_ORIGINS adds more
# (comma-separated), e.g. a custom domain.
_DEFAULT_ORIGINS = ["https://download-seven-alpha.vercel.app"]
_ALLOWED_ORIGINS = _DEFAULT_ORIGINS + [
    o.strip().rstrip("/") for o in os.environ.get("ALLOWED_ORIGINS", "").split(",") if o.strip()
]
if _ALLOWED_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_ALLOWED_ORIGINS,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
        expose_headers=["Content-Length", "Content-Disposition", "Retry-After"],
    )


def _error_response(exc: AppError) -> JSONResponse:
    headers = {"Cache-Control": "no-store"}
    if exc.retry_after:
        headers["Retry-After"] = str(exc.retry_after)
    return JSONResponse(
        {"error": {"code": exc.code, "message": exc.message,
                   "hint": exc.hint, "retry_after": exc.retry_after}},
        status_code=exc.status,
        headers=headers,
    )


@app.exception_handler(AppError)
async def _handle_app_error(_: Request, exc: AppError) -> JSONResponse:
    return _error_response(exc)


@app.exception_handler(RequestValidationError)
async def _handle_validation(_: Request, __: RequestValidationError) -> JSONResponse:
    return _error_response(make("bad_request"))


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(
        INDEX_HTML,
        media_type="text/html; charset=utf-8",
        headers={"Cache-Control": "no-cache"},
    )


@app.get("/api/health")
async def health() -> JSONResponse:
    return JSONResponse(
        {
            "ok": True,
            "yt_dlp": YTDLP_VERSION,
            "proxy": bool(PROXY_TEMPLATE),
            "ffmpeg": bool(FFMPEG_PATH),
            "cookies": cookies_readable(),
            "limits": {
                "max_duration_min": MAX_DURATION_MIN,
                "downloads_per_window": RATE_LIMIT_DOWNLOADS,
                "window_min": RATE_LIMIT_WINDOW_MIN,
            },
        },
        headers={"Cache-Control": "no-store"},
    )


class InfoRequest(BaseModel):
    url: str


@app.post("/api/info")
async def api_info(body: InfoRequest, request: Request) -> JSONResponse:
    video_id = extract_video_id(body.url)
    ip = client_ip(request)
    ok, wait = info_limiter.acquire(ip)
    if not ok:
        raise make("info_rate_limited", retry_after=wait, wait=wait_text(wait))

    try:
        await acquire_slot(info_sem)
    except AppError:
        info_limiter.refund(ip)
        raise
    fut = spawn(info_sem, lambda: run_info(video_id))
    data = await asyncio.shield(fut)
    return JSONResponse(data, headers={"Cache-Control": "no-store"})


def safe_filename(title: str, ext: str) -> str:
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "", title or "")
    name = re.sub(r"\s+", " ", name).strip(" .")[:120].strip(" .") or "download"
    return f"{name}.{ext}"


class CleanupFileResponse(FileResponse):
    """FileResponse that removes the job folder once sending ends, however it ends."""

    def __init__(self, *args: Any, job_dir: Path, **kwargs: Any) -> None:
        super().__init__(*args, background=BackgroundTask(remove_dir, job_dir), **kwargs)
        self._job_dir = job_dir

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:  # also covers client disconnects, where background tasks may not run
            remove_dir(self._job_dir)


@app.get("/api/download")
async def api_download(
    request: Request,
    url: str = Query(..., max_length=2048),
    format: str = Query("mp3", max_length=8),
    quality: str = Query("", max_length=8),
) -> FileResponse:
    video_id = extract_video_id(url)
    spec = parse_spec(format, quality)
    if not FFMPEG_PATH:
        raise make("ffmpeg_missing")

    ip = client_ip(request)
    ok, wait = dl_limiter.acquire(ip)
    if not ok:
        raise make(
            "rate_limited", retry_after=wait, wait=wait_text(wait),
            limit=RATE_LIMIT_DOWNLOADS, minutes=RATE_LIMIT_WINDOW_MIN,
        )
    if GLOBAL_DOWNLOADS_PER_HOUR:
        ok, wait = global_limiter.acquire("all")
        if not ok:
            dl_limiter.refund(ip)
            raise make("global_limited", retry_after=wait, wait=wait_text(wait))

    def refund() -> None:
        dl_limiter.refund(ip)
        if GLOBAL_DOWNLOADS_PER_HOUR:
            global_limiter.refund("all")

    try:
        await acquire_slot(job_sem)
    except AppError:
        refund()
        raise

    job_dir: Path | None = None
    try:
        job_dir = new_job_dir()
        fut = spawn(job_sem, lambda: run_download(video_id, spec, job_dir))
    except BaseException:
        job_sem.release()
        remove_dir(job_dir)
        raise

    try:
        result = await asyncio.shield(fut)
    except asyncio.CancelledError:
        # Client left while yt-dlp is still running: clean up once the thread ends.
        fut.add_done_callback(lambda _f: remove_dir(job_dir))
        raise
    except BaseException:
        remove_dir(job_dir)
        raise

    try:
        return CleanupFileResponse(
            result.path,
            media_type=MEDIA_TYPES.get(result.ext, "application/octet-stream"),
            filename=safe_filename(result.title, result.ext),
            job_dir=job_dir,
            headers={"Cache-Control": "no-store"},
        )
    except BaseException:
        remove_dir(job_dir)
        raise
