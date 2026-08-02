# Use a stable Python 3.11 image[cite: 3]
FROM python:3.11-slim[cite: 3]

# Prevent Python from writing pyc files and enable unbuffered logs[cite: 3]
ENV PYTHONDONTWRITEBYTECODE=1[cite: 3]
ENV PYTHONUNBUFFERED=1[cite: 3]

WORKDIR /app[cite: 3]

# Install system deps required for building wheels if needed (kept minimal)[cite: 3]
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*[cite: 3]

# Copy requirements and install[cite: 3]
COPY requirements.txt /app/requirements.txt[cite: 3]
RUN python -m pip install --upgrade pip setuptools wheel \
 && pip install -r /app/requirements.txt[cite: 3]

# Copy app source[cite: 3]
COPY . /app[cite: 3]

# Create a non-root user (optional safety)[cite: 3]
RUN useradd --create-home botuser || true[cite: 3]
USER botuser[cite: 3]

# Expose port for Render (Render sets PORT env var)[cite: 3]
EXPOSE 10000[cite: 3]

# Default command - run via the module entrypoint; main.py will bind to $PORT[cite: 3]
CMD ["python", "-m", "src.main"][cite: 3]
