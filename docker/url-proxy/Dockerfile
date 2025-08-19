FROM python:3.9-slim

LABEL maintainer="ClearML URL Proxy Service"
LABEL description="Fixes URL encoding issues for ClearML file server"

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the proxy service
COPY url_proxy.py .

# Create non-root user for security
RUN groupadd -r clearml && useradd -r -g clearml clearml
RUN chown -R clearml:clearml /app
USER clearml

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8082/health || exit 1

# Expose proxy port
EXPOSE 8082

# Start the proxy service
CMD ["python", "url_proxy.py"]
