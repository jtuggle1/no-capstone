FROM python:3.9-slim

WORKDIR /app
RUN pip install --no-cache-dir kafka-python Faker prometheus_client
#RUN pip install kafka-python
#RUN pip install faker
#RUN pip install --no-cache-dir prometheus_client
COPY . .
EXPOSE 8001
CMD ["python", "producer.py"]



