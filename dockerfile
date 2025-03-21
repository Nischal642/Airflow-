FROM apache/airflow:latest

# Install system dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends libpq-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python packages
USER airflow
RUN pip install --no-cache-dir requests beautifulsoup4 psycopg2-binary

CMD ["airflow", "standalone"]
