# Fetchbox

Paste a YouTube link, pick audio or video, download the file. Everything runs on
one hosted server (FastAPI + yt-dlp + FFmpeg), so visitors need nothing installed.
No accounts, no database, no stored user data: each job gets a temp folder that is
deleted as soon as the response has been sent, or as soon as anything fails.

```
main.py           FastAPI app (API + serves index.html)
index.html        the whole frontend (vanilla HTML/CSS/JS)
Dockerfile        python:3.12-slim + ffmpeg + deno, non-root
requirements.txt  fastapi, uvicorn[standard], yt-dlp[default]
render.yaml       optional Render blueprint
```

## Why a proxy is needed

YouTube blocks most datacenter IP ranges, Render's included. Without a proxy,
yt-dlp fails with "Sign in to confirm you're not a bot". Fetchbox sends **all**
YouTube traffic through a residential proxy (`YTDLP_PROXY`) so requests look like
they come from a normal home connection. It does not try to bypass DRM, and
DRM-protected formats are skipped.

## 1. Get a residential proxy

Buy residential proxy access from a provider that offers an HTTP(S) endpoint
(host, port, username, password), billed per GB. Pick one that sources its IPs
with consent, and check its terms allow this use.

Your provider gives you a URL like:

```
http://USERNAME:PASSWORD@HOST:PORT
```

If the password contains special characters (`@ : / ? #`), percent-encode them.

**Rotating vs. sticky.** YouTube's media URLs are tied to the IP that requested
them. If the proxy switches IP between the lookup and the download inside one job,
the download can fail with HTTP 403. There are two ways to handle this:

- **Sticky session (recommended).** Most providers let you pin an exit IP by
  adding a session id to the username. Put the placeholder `{session}` where the
  id goes and Fetchbox substitutes a fresh random id per job, so each job keeps
  one IP and a retry gets a new one. The exact username format differs by
  provider, so check their docs. Shape of the value:
  `http://USER-session-{session}:PASSWORD@HOST:PORT`
- **Rotating gateway.** Works too, but expect occasional failures; Fetchbox
  already retries once automatically.

**Cost.** Proxy traffic is billed per GB, and Fetchbox sends the video data itself
through the proxy. As a rough guide, 10 minutes of audio is a few MB and 10 minutes of
1080p video is on the order of 100 MB. The page shows an estimate for each choice.
Use the limits below to cap spending.

## 2. Deploy on Render

1. Push this folder to a Git repository.
2. In Render: **New > Web Service**, pick the repo, runtime **Docker**
   (or use **New > Blueprint** with `render.yaml`).
3. Under **Environment**, add `YTDLP_PROXY` with your proxy URL.
   Optionally adjust the other variables (table below).
4. Deploy. Render sets `$PORT`; the container defaults to 10000.
5. Open `/api/health`. You should see `"proxy": true` and a yt-dlp version.

Run it as a **single instance** (rate-limit counters are in memory).

## 3. Optional: cookies.txt

Cookies help with age-restricted videos and with stubborn "not a bot" blocks.

1. Export a `cookies.txt` (Netscape format) from a browser session on a
   **throwaway Google account**, not your main one. Automated use can get an
   account flagged.
2. In Render: **Environment > Secret Files > Add**, filename `cookies.txt`
   (it is mounted at `/etc/secrets/cookies.txt`).
3. Redeploy. `/api/health` should show `"cookies": true`.

Render secret files are read-only, so for every job Fetchbox copies the file into
that job's temp folder, passes the copy to yt-dlp, and deletes it as soon as
yt-dlp is done. Cookies expire; if they stop helping, export a fresh file.

## 4. Updating yt-dlp

YouTube changes often, and yt-dlp releases fixes quickly. `requirements.txt` is
unpinned, so a fresh build installs the latest release. Docker caches the install
layer, so a plain redeploy may reuse an old one. In Render use
**Manual Deploy > Clear build cache & deploy**. Do this first whenever downloads
start failing for no obvious reason. `/api/health` shows the installed version.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `YTDLP_PROXY` | (unset) | Proxy URL. May contain `{session}`. Never logged. |
| `MAX_DURATION_MIN` | `60` | Reject longer videos (protects proxy bandwidth). |
| `RATE_LIMIT_DOWNLOADS` | `5` | Downloads per visitor per window. |
| `RATE_LIMIT_INFO` | `30` | Lookups per visitor per window. |
| `RATE_LIMIT_WINDOW_MIN` | `10` | Window length in minutes. |
| `GLOBAL_DOWNLOADS_PER_HOUR` | `100` | Site-wide cap per hour; `0` disables. |
| `MAX_CONCURRENT_JOBS` | `2` | Simultaneous downloads. Others wait up to `QUEUE_WAIT_SEC` (20), then see "busy". |
| `JOB_TIMEOUT_SEC` | `900` | Stop a job that runs this long. |
| `TRUSTED_PROXY_HOPS` | `1` | See the note below. |
| `COOKIES_FILE` | `/etc/secrets/cookies.txt` | Where the optional secret file lives. |
| `DEBUG_LOG_ERRORS` | (off) | Set to `1` to log unrecognised yt-dlp errors. Messages may contain video ids. |

**Visitor identification.** Rate limits are per visitor IP, read from
`X-Forwarded-For`, counting `TRUSTED_PROXY_HOPS` entries from the right so a
client can't spoof the header. If every visitor seems to share one limit, your
platform appends more than one proxy address. Try `2`. The global cap protects
your proxy budget either way.

## Behaviour

- Only `youtube.com`, `music.youtube.com` and `youtu.be` links are accepted. The
  server rebuilds a clean single-video URL from the video id, so playlist
  parameters are ignored and no other host can be reached.
- Formats: MP3 (128/192/320 kbps), M4A, Opus, WAV, MP4 (H.264 + AAC preferred),
  WebM. Video heights come from what the video offers. YouTube offers H.264 only up
  to 1080p; above that, MP4 files hold VP9 or AV1.
- Temp files: one folder per job, removed after sending, on failure, on client
  disconnect, and by a janitor sweep as a safety net. yt-dlp's cache is disabled.
  Nothing is logged about links or visitors.
- Transient failures (connection drops, proxy hiccups, "not a bot") are retried once
  on a fresh proxy connection.

## Acceptance checklist

- [ ] With `YTDLP_PROXY` set, a download works from the Render deployment.
- [ ] Without it, the page shows the "No residential proxy" notice and lookups
      explain the problem instead of showing a raw error.
- [ ] After a download, `/tmp/fetchbox-jobs` is empty (Render shell: `ls /tmp/fetchbox-jobs`).
- [ ] Break a download on purpose (for example a private video): no job folder remains.
- [ ] Six quick downloads from one browser trigger the friendly rate-limit message.
- [ ] A video longer than `MAX_DURATION_MIN` is refused with a clear message.

## Run locally

```bash
pip install -r requirements.txt   # also needs ffmpeg (and deno) on your PATH
YTDLP_PROXY="" uvicorn main:app --port 10000
```

From a home connection a proxy is usually unnecessary; the page will warn about it anyway.

## Responsible use

Only download content you own, have permission to save, or that is licensed for
it. Downloading may violate YouTube's Terms of Service and copyright law where
you live. The operator of a public instance is responsible for how it is used.
