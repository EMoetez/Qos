FROM python:3.11-slim

WORKDIR /app
COPY kibana-objects.ndjson .
COPY import_objects.py .

RUN pip install requests

CMD ["python", "import_objects.py"]
