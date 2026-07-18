# syntax=docker/dockerfile:1
# ---- build stage -----------------------------------------------------------
FROM python:3.11-slim AS build
WORKDIR /app
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1

COPY pyproject.toml requirements.txt README.md ./
COPY src ./src
RUN pip install --prefix=/install .

# ---- runtime stage ---------------------------------------------------------
FROM python:3.11-slim AS runtime
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TRADEWATCH_HOST=0.0.0.0 \
    TRADEWATCH_PORT=8000

# Run as a non-root user.
RUN useradd --create-home --uid 10001 appuser

COPY --from=build /install /usr/local
COPY config ./config

USER appuser
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health').status==200 else 1)"

CMD ["tradewatch", "serve"]
