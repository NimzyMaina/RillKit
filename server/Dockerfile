# rillkit-server with the dashboard built in. Build from the repository root:
#   docker build -f server/Dockerfile -t rillkit/server .

FROM node:22-alpine AS dashboard
WORKDIR /dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ ./
RUN npm run build

FROM python:3.12-slim AS server
COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1 UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY sdks/python /app/sdks/python
COPY server /app/server
RUN cd /app/server && uv sync --frozen --no-dev --no-editable
COPY --from=dashboard /dashboard/dist /app/static
RUN useradd --system --uid 10001 rillkit
USER rillkit
ENV PATH=/app/server/.venv/bin:$PATH RILLKIT_SERVER_STATIC_DIR=/app/static
LABEL org.opencontainers.image.title="rillkit-server" \
      org.opencontainers.image.licenses="FSL-1.1-ALv2" \
      org.opencontainers.image.source="https://github.com/NimzyMaina/RillKit"
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=6 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/api/health')"
CMD ["rillkit-server"]
