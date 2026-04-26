FROM node:20-slim AS assets

WORKDIR /app

COPY package.json package-lock.json tailwind.config.js /app/
COPY src /app/src
COPY agent_operations_viewer /app/agent_operations_viewer

RUN npm ci
RUN npx tailwindcss -i ./src/tailwind.css -o ./agent_operations_viewer/static/app.css --minify

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/requirements.txt

COPY agent_operations_viewer /app/agent_operations_viewer
COPY --from=assets /app/agent_operations_viewer/static/app.css /app/agent_operations_viewer/static/app.css

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD python -c "import json, urllib.request; json.load(urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=3))"

CMD ["python", "-m", "agent_operations_viewer", "serve", "--no-sync"]
