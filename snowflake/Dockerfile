FROM python:3.9-slim

RUN pip install --no-cache-dir snowflake-connector-python snowflake-sqlalchemy

WORKDIR /app

CMD ["python", "-c", "import snowflake.connector; print('Snowflake CLI container is ready')"] 