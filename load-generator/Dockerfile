FROM python:3.9-slim

WORKDIR /app

COPY . .
RUN pip install kafka-python
#--no-cache-dir -r requirements.txt

CMD ["python", "-u", "producer.py"]