FROM python:3.9-slim
WORKDIR /app
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health').read()" || exit 1
CMD gunicorn \
    --bind 0.0.0.0:8080 \
    --workers $(python -c "import multiprocessing; print(min(multiprocessing.cpu_count() * 2 + 1, 8))") \
    --threads 2 \
    --timeout 120 \
    --worker-class sync \
    --worker-tmp-dir /dev/shm \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    --graceful-timeout 30 \
    --keep-alive 5 \
    app:app
