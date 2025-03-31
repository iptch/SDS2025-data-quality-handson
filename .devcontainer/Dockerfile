# .devcontainer/Dockerfile
FROM python:3.12-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
    git \
    curl \
    gnupg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js SQLite package globally
RUN npm install -g sqlite3@5.1.7

# Install Python dependencies
WORKDIR /workspace
COPY requirements.txt .
RUN pip install -r requirements.txt

# Create non-root user
RUN groupadd --gid 1000 vscode \
    && useradd --uid 1000 --gid 1000 -m vscode \
    # Create directory structure for the user
    && mkdir -p /home/vscode/.local/bin \
    && chown -R vscode:vscode /home/vscode/.local
    
# Set PATH for vscode user
ENV PATH="/home/vscode/.local/bin:${PATH}"

# Switch to non-root user
USER vscode