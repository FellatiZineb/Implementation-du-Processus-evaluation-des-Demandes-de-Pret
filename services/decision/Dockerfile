FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY services/decision/app /app/app
COPY shared /app/shared

ENV PYTHONPATH=/app

CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8000"]
