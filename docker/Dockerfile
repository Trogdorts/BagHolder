FROM python:3.12-slim

ARG APP_VERSION="0.1.0-alpha.1"
ARG BAGHOLDER_UID=1000
ARG BAGHOLDER_GID=1000

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    BAGHOLDER_DATA=/app/data

LABEL org.opencontainers.image.title="BagHolder" \
      org.opencontainers.image.version="$APP_VERSION" \
      org.opencontainers.image.description="FastAPI application for visualizing trading performance" \
      org.opencontainers.image.licenses="MIT"

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

RUN groupadd --system --gid "$BAGHOLDER_GID" bagholder \
    && useradd --system --gid bagholder --uid "$BAGHOLDER_UID" --create-home bagholder \
    && mkdir -p /app/data \
    && chown -R bagholder:bagholder /app

USER bagholder

EXPOSE 8012
VOLUME ["/app/data"]

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8012", "--proxy-headers"]
