# Same python:3.11-slim base as etl/Dockerfile on purpose - keeping base
# images consistent across a project avoids subtle "works in one container,
# breaks in another" bugs caused by mismatched system libraries.
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

CMD ["python", "producer.py"]
