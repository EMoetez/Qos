FROM python:3.11-slim

WORKDIR /app
COPY create_index.py .

RUN pip install requests

CMD ["python", "create_index.py"]
