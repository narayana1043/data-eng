FROM python:3.12.3-slim

# System deps (minimal, safe for sentence-transformers + psycopg2-binary)
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Prevent Python from writing .pyc files & buffer logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV VENV_PATH=/venv
ENV PATH="/venv/bin:$PATH"
ENV PIP_CACHE_DIR=/pipcache
RUN mkdir -p /pipcache && chmod -R 777 /pipcache

# Set work directory
WORKDIR /app

# Copy dependency list first (better caching)
COPY requirements.txt /
COPY entrypoint.sh /
RUN chmod +x /entrypoint.sh

# Copy scripts
COPY ./scripts/ /scripts/

ENTRYPOINT ["/entrypoint.sh"]
