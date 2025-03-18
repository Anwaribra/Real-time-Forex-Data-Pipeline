FROM python:3.9

WORKDIR /app

RUN apt-get update && apt-get install -y netcat-traditional


COPY scripts/wait-for-kafka.sh /app/scripts/
RUN chmod +x scripts/wait-for-kafka.sh


VOLUME ["/app"]

CMD ["./scripts/wait-for-kafka.sh", "&&", "python", "main_pipeline.py"] 