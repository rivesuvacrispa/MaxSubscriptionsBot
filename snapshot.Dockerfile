FROM python:3.13-slim

WORKDIR /app

COPY requirements/snapshot.txt .

RUN pip install --no-cache-dir -r snapshot.txt

COPY . .

CMD ["python", "snapshot.py"]
