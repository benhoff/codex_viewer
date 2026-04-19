FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/requirements.txt

COPY codex_session_viewer /app/codex_session_viewer

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD python -c "import json, urllib.request; json.load(urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=3))"

CMD ["python", "-m", "codex_session_viewer", "serve", "--no-sync"]

