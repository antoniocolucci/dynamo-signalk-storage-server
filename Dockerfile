FROM python:3.8-slim-buster

WORKDIR /dynamo-storage

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

RUN MKDIR -p data/media
RUN MKDIR -p data/scratch
RUN MKDIR -p data/trash

COPY keys keys
COPY app.py app.py
COPY config.cfg config.cfg
COPY queues.py queues.py
COPY storage.py storage.py
COPY store.py store.py
COPY tasks.py tasks.py
COPY uncompress.py uncompress.py

COPY dynamo-storage.sh dynamo-storage.sh

ENTRYPOINT ["./dynamo-storage.sh"]