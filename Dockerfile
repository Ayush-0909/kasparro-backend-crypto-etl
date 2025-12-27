FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["bash", "-c", "python -c \"from core.database import Base, engine; Base.metadata.create_all(bind=engine)\" && python -c \"from services.etl_service import run_etl; run_etl()\" && uvicorn api.main:app --host 0.0.0.0 --port 8000"]
