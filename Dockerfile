# Use a stable Python 3.11 image
FROM python:3.11-slim

# Prevent Python from writing pyc files and enable unbuffered logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system deps required for building wheels if needed (kept minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip setuptools wheel \
 && pip install -r /app/requirements.txt

# Copy app source
COPY . /app

# Create a non-root user (optional safety)
RUN useradd --create-home botuser || true
USER botuser

# Expose port for Render (Render sets PORT env var)
EXPOSE 10000

# Default command - run via the module entrypoint; main.py will bind to $PORT
CMD ["python", "-m", "src.main"]
