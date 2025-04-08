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


# Create non-root user
RUN groupadd --gid 1000 vscode \
    && useradd --uid 1000 --gid 1000 -m vscode \
    # Create directory structure for the user
    && mkdir -p /home/vscode/.local/bin \
    && chown -R vscode:vscode /home/vscode/.local

# Switch to non-root user
USER vscode

# Create and activate virtual environment
RUN python -m venv /home/vscode/.venv
ENV PATH="/home/vscode/.venv/bin:${PATH}"
ENV VIRTUAL_ENV="/home/vscode/.venv"

# Install Python dependencies
WORKDIR /workspace
COPY --chown=vscode:vscode requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Install and configure ipykernel
RUN pip install ipykernel && \
    python -m ipykernel install --user --name=workshop-venv --display-name="Workshop Environment"

# Set PATH for vscode user
ENV PATH="/home/vscode/.local/bin:${PATH}"