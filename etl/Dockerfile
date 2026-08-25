FROM python:3.11-slim

WORKDIR /app

# gcc needed by psycopg2-binary
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY extract.py clean.py load.py ./

CMD ["python", "load.py"]
