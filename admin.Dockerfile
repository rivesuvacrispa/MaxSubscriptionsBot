FROM python:3.13-slim

WORKDIR /app

COPY requirements/admin.txt .

RUN pip install --no-cache-dir -r admin.txt

COPY . .

CMD ["uvicorn", "admin:app", "--reload", "--workers", "4", "--host", "0.0.0.0", "--port", "8000"]