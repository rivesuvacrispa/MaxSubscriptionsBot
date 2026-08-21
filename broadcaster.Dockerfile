FROM python:3.13-slim

WORKDIR /app

COPY requirements/broadcaster.txt .

RUN pip install --no-cache-dir -r broadcaster.txt

COPY . .

CMD ["python", "broadcaster.py"]
