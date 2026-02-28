FROM --platform=$BUILDPLATFORM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY videos.json .
COPY start.sh .

RUN chmod +x start.sh

ENV PYTHONPATH=/app

CMD ["./start.sh"]
