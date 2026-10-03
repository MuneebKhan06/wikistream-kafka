# The dashboard: the React app and the API that serves it, in one image.
#
# Stage one builds the frontend with Node. Stage two is a slim Python image
# with only the API's code and the built files, so Node never ships.

FROM node:22-alpine AS frontend
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm test && npm run build


FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    WEB_HOST=0.0.0.0 \
    WEB_PORT=8050

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# The API imports the shared config and models, and the page record format
# the page-state processor writes. Nothing else from the pipeline.
COPY common/ common/
COPY processors/__init__.py processors/page_latest.py processors/
COPY web/ web/
COPY benchmarks/ benchmarks/
COPY --from=frontend /app/frontend/dist frontend/dist

RUN useradd --system --no-create-home dashboard
USER dashboard

EXPOSE 8050
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8050/api/health', timeout=4)"

CMD ["python", "-m", "web"]
