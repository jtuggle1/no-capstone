FROM python:3.10-slim
WORKDIR /app
COPY . .
RUN pip install kafka-python
RUN pip install faker
CMD ["python", "kafka_producer.py"]
