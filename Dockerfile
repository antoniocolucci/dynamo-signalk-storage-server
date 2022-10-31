FROM python:3.8-slim-buster

WORKDIR /dynamo-signalk-storage-server

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

RUN mkdir -p data/media
RUN mkdir -p data/scratch
RUN mkdir -p data/trash

COPY data/keys keys
COPY config.cfg.sample config.cfg
COPY app/queues.py queues.py
COPY app/storage.py storage.py
COPY app/tasks.py tasks.py
COPY app/uncompress.py uncompress.py
COPY static/ static/
COPY templates/ templates/
COPY app.py app.py

COPY dynamo-signalk-storage-server.sh dynamo-signalk-storage-server.sh

ENTRYPOINT ["./dynamo-signalk-storage-server.sh"]
