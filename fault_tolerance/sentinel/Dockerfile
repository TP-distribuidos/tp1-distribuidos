FROM python:3.11-slim

WORKDIR /app

# Install Docker CLI for container management
RUN apt-get update && \
    apt-get install -y curl && \
    curl -fsSL https://get.docker.com -o get-docker.sh && \
    sh get-docker.sh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* get-docker.sh

# Copy application code
COPY . .

# Install dependencies
RUN pip install docker

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
