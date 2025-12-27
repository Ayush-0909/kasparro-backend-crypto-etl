FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy dependency file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Expose API port
EXPOSE 8000

# Start ETL and API
CMD ["sh", "-c", "python -c 'from services.etl_service import run_etl; run_etl()' && uvicorn api.main:app --host 0.0.0.0 --port 8000"]
