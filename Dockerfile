FROM python:3.8-slim-buster

WORKDIR /dynamo-signalk-storage-server

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY data data
COPY config.cfg.sample config.cfg
COPY app app
COPY dynamo-signalk-storage-server.py dynamo-signalk-storage-server.py

COPY dynamo-signalk-storage-server.sh dynamo-signalk-storage-server.sh

ENTRYPOINT ["./dynamo-signalk-storage-server.sh"]
