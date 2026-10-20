# deno is needed by yt-dlp to solve YouTube's JavaScript challenges
FROM denoland/deno:bin AS deno

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PORT=10000

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=deno /deno /usr/local/bin/deno

RUN useradd --create-home --uid 10001 app
WORKDIR /app

# Unpinned on purpose: every fresh build pulls the latest yt-dlp release.
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY main.py index.html ./

USER app
EXPOSE 10000

# One worker only: rate-limit counters live in memory.
CMD ["sh", "-c", "exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-10000} --no-server-header"]
