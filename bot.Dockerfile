FROM python:3.13-slim

WORKDIR /app

COPY requirements/bot.txt .


RUN pip install --no-cache-dir -r bot.txt

COPY . .

CMD ["python", "bot.py"]