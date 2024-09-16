FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY data.csv load_generator.py ./

CMD ["python", "load_generator.py"]