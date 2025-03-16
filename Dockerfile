FROM python:3.9

WORKDIR /app

# Install netcat for health checks only
RUN apt-get update && apt-get install -y netcat-traditional

# Don't copy or install requirements since we're using local environment
COPY scripts/wait-for-kafka.sh /app/scripts/
RUN chmod +x scripts/wait-for-kafka.sh

# Mount the local directory instead of copying files
VOLUME ["/app"]

CMD ["./scripts/wait-for-kafka.sh", "&&", "python", "main_pipeline.py"] 