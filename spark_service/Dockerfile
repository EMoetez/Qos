FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark_service.py .

EXPOSE 5000

CMD ["python", "spark_service.py"]