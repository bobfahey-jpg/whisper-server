FROM runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04

# Install Microsoft ODBC Driver 18 for SQL Server (needed by pyodbc)
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl -sSL https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev \
    && apt-get clean

# Install all Python deps at build time
RUN pip install --no-cache-dir \
    faster-whisper \
    fastapi \
    uvicorn \
    requests \
    python-multipart \
    pyodbc \
    azure-storage-blob

# Copy scripts
WORKDIR /workspace
COPY whisper_server.py /workspace/whisper_server.py
COPY pod_worker.py /workspace/pod_worker.py

# Pre-download large-v3-turbo into the image layer (no GPU needed for download).
RUN python3 -c "from faster_whisper import WhisperModel; WhisperModel('large-v3-turbo', device='cpu', compute_type='int8'); print('Model cached.')"

# Expose whisper server port (still available if needed)
EXPOSE 8765

# Default: run autonomous worker (pulls from Azure SQL, writes to Azure Blob)
# Override with: docker run ... --entrypoint uvicorn ... (for server mode)
CMD ["python3", "/workspace/pod_worker.py"]
