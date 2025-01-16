FROM python:3.9-slim

WORKDIR /app

COPY ../python-scripts/transaction_generator.py .

RUN pip install requests

CMD ["python", "transaction_generator.py"]
