# ---------- build stage ----------
FROM python:3.11-slim AS build

# Системные либы, нужные Chromium-у
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget gnupg ca-certificates fonts-liberation libgtk-3-0 \
        libnss3 libdbus-glib-1-2 libxdamage1 libxshmfence1 libasound2 && \
    rm -rf /var/lib/apt/lists/*

# Копируем зависимости Python
COPY pyproject.toml poetry.lock /tmp/src/
WORKDIR /tmp/src
RUN pip install --upgrade pip \
 && pip install "poetry>=1.8" \
 && poetry export --without-hashes -o requirements.txt \
 && pip install -r requirements.txt

# Скачиваем Chromium один раз, прямо в образ
RUN playwright install --with-deps chromium

# ---------- runtime stage ----------
FROM python:3.11-slim

ENV PLAYWRIGHT_BROWSERS_PATH=/usr/local/share/pw-browsers

# Копируем всё, что собрано, плюс исходники
COPY --from=build /usr/local /usr/local
COPY . /app
WORKDIR /app

CMD ["python", "main.py"]
