FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt ./
RUN apt-get update && apt-get install -y python3-dev libkrb5-dev gcc
RUN pip install --no-cache-dir -r requirements.txt

COPY data_sink.py ./

CMD ["python", "data_sink.py"]