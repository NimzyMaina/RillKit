# The demo workers and producer. Build from the repository root (see compose.yaml).
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /usr/local/bin/uv
WORKDIR /app
COPY sdks/python /app/sdks/python
RUN uv pip install --system --no-cache "/app/sdks/python[control]"
COPY examples/demo/worker.py /app/worker.py
RUN useradd --system --uid 10001 demo
LABEL org.opencontainers.image.licenses="Apache-2.0"
USER demo
CMD ["python", "/app/worker.py"]
