# PINNED to a specific Airflow version + Python version combo that is
# officially tested together (2.9.3 on Python 3.11). Mixing an untested
# Airflow/Python combo is the #1 cause of "Docker images don't work
# together" pain - always start from the exact tag in Airflow's own
# compatibility matrix instead of "latest".
FROM apache/airflow:2.9.3-python3.11

# Airflow's image runs as a non-root "airflow" user by default, and pip
# installs must happen as that same user - switching to root here just to
# copy files, then back to airflow (below) to install, is the documented
# safe pattern for extending this image.
USER root
COPY etl/requirements.txt /requirements.txt
USER airflow

# We reuse the SAME requirements.txt as etl/Dockerfile (build context is
# the project root - see docker-compose.yml) so the DAG's Python
# environment matches the batch ETL container exactly - no
# "works in ETL container, fails in Airflow" surprises.
RUN pip install --no-cache-dir --user -r /requirements.txt
