FROM python:3.12-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim

COPY --from=builder /install /usr/local

WORKDIR /app
COPY app/ orchestrator/app/
RUN touch orchestrator/__init__.py

RUN useradd -r appuser
USER appuser

EXPOSE 8000

CMD ["uvicorn", "orchestrator.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
