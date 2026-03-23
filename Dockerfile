FROM runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04

# ODBC Driver 18 + openssh-server
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl -sSL https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev openssh-server \
    && apt-get clean \
    && mkdir -p /var/run/sshd \
    && echo "PermitRootLogin yes" >> /etc/ssh/sshd_config \
    && echo "PasswordAuthentication no" >> /etc/ssh/sshd_config

RUN pip install --no-cache-dir faster-whisper fastapi uvicorn requests python-multipart pyodbc azure-storage-blob opencensus-ext-azure

WORKDIR /workspace
COPY whisper_server.py /workspace/whisper_server.py
COPY pod_worker.py /workspace/pod_worker.py
COPY start.sh /workspace/start.sh
RUN chmod +x /workspace/start.sh

# Pre-bake model — eliminates download on pod startup (~55s boot vs 8 min)
RUN python3 -c "from faster_whisper import WhisperModel; WhisperModel('large-v3-turbo', device='cpu', compute_type='int8'); print('Model cached.')"

EXPOSE 8765 22

CMD ["/workspace/start.sh"]

