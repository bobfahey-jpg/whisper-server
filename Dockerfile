FROM runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04

# Install all Python deps at build time
RUN pip install --no-cache-dir \
    faster-whisper \
    fastapi \
    uvicorn \
    requests \
    python-multipart

# Bake whisper_server.py into the image
WORKDIR /workspace
COPY whisper_server.py /workspace/whisper_server.py

# Pre-download large-v3-turbo into the image layer (no GPU needed for download).
# Uses CPU + int8 to load the model during build — same files used at GPU runtime.
RUN python3 -c "from faster_whisper import WhisperModel; WhisperModel('large-v3-turbo', device='cpu', compute_type='int8'); print('Model cached.')"

# Expose whisper port
EXPOSE 8765

# WHISPER_API_KEY must be set as env var (RunPod template env, docker run -e, or compose)
CMD ["uvicorn", "whisper_server:app", "--host", "0.0.0.0", "--port", "8765", "--workers", "4"]
