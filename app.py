import logging
import os
import shutil
import sys
import tempfile
import time
import werkzeug
import json
from celery import Celery
from flask import Flask, Blueprint, url_for, jsonify, current_app, abort, send_file, render_template
from flask_restplus import Api, Resource, reqparse
from os import listdir
from os.path import isdir, isfile, join
from sqlalchemy import create_engine
from flask_cors import CORS

from queues import Queues
from tasks import process_file_task, process_file_queue

log = logging.getLogger('app')
log.setLevel(logging.DEBUG)  # DEBUG

fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
h = logging.StreamHandler()
h.setFormatter(fmt)
log.addHandler(h)

file_upload = reqparse.RequestParser()
file_upload.add_argument('file',
                         type=werkzeug.datastructures.FileStorage,
                         location='files',
                         required=True,
                         help='DYNAMO log.gz.enc file')
file_upload.add_argument('sessionId',
                         required=True,
                         help="Session unique identifier")

app = Flask(__name__)
CORS(app)

app.config.from_pyfile('config.cfg', silent=True)  # instance-folders configuration

celery = Celery(app.name, backend=app.config['CELERY_RESULT_BACKEND'], broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)


def myworker(queue_item):
    print("myworker:" + queue_item)


def get_conf():

    conf = {
        "media_root": app.config.get("MEDIA_ROOT"),
        "scratch_root": app.config.get("SCRATCH_ROOT"),
        "trash_root": app.config.get("TRASH_ROOT"),
        "connection_string": app.config.get("CONNECTION_STRING"),
        "private_key_filename": app.config.get("PRIVATE_KEY_FILENAME"),
        "public_key_filename": app.config.get("PUBLIC_KEY_FILENAME"),
        "public_key_root": app.config.get("PUBLIC_KEY_ROOT"),
        "queue_concurrency": 1
    }

    return conf


# Define the queue
queues = Queues(process_file_queue, get_conf()["concurrency"])

# Start the queue
queues.start()


def process_files():
    log.info("Start " + time.strftime("%A, %d. %B %Y %I:%M:%S %p") + "...")

    conf = get_conf()
    media_root = conf["media_root"]

    directories = [f for f in listdir(media_root) if isdir(join(media_root, f))]
    for directory in directories:
        directory_root = media_root + "/" + directory
        files = [f for f in listdir(directory_root) if isfile(join(directory_root, f))]
        for file_item in files:
            log.info("Processing:" + media_root + "," + directory + "," + file_item)
            # process_file_task.delay(vessel, file_item, conf)
            queues.enqueue({"directory": directory, "file_item": file_item, "conf": conf})

    queues.join()
    log.info("... " + time.strftime("%A, %d. %B %Y %I:%M:%S %p") + " finish.")


api = Api(app)
process_files()


@api.route('/publickey')
class PublicKey(Resource):
    def get(self):
        conf = get_conf()
        return send_file(conf["public_key_filename"], as_attachment=True)


@api.route('/upload/<selfId>')
class ParcelUpload(Resource):
    @api.expect(file_upload)
    def post(self, selfId):
        args = file_upload.parse_args()

        if args['file'].mimetype == 'application/octet-stream':
            destination = os.path.join(current_app.config.get('MEDIA_ROOT'), str(selfId) + "/")
            if not os.path.exists(destination):
                os.makedirs(destination)

            done = False
            while not done:
                temp_name = next(tempfile._get_candidate_names())
                file_path = '%s%s%s' % (destination, temp_name, '.log.gz.enc')
                if os.path.isfile(file_path) is False:
                    done = True

            args['file'].save(file_path)

            conf = get_conf()
            # task = process_file_task.delay(selfId, file_path, conf)
            queues.enqueue({"directory": selfId, "file_item": temp_name + '.log.gz.enc', "conf": conf})
        else:
            return {"error": "File mimetype must be application/octet-stream"}, 422

        return {'status': 'Done'}


@api.route('/lastPosition')
class LastPosition(Resource):
    def get(self):
        positions = []

        conf = get_conf()

        connection_string = conf["connection_string"]
        engine = create_engine(connection_string, echo=False)
        conn = engine.connect()

        try:
            result = conn.execute(
                "SELECT context, timestamp, value FROM public.navigation_position np1 WHERE timestamp = ( SELECT MAX( np2.timestamp ) FROM public.navigation_position np2 WHERE np1.context = np2.context ) ORDER BY context;")
            for row in result:
                positions.append({
                    "id": row[0].split(":")[-1],
                    "timestamp": str(row[1]),
                    "value": row[2]
                })
            conn.close()
        except:
            conn.close()

        return positions


@app.route('/map')
def index():
    return render_template('index.html')


if __name__ == '__main__':
    app.run(use_reloader=False)
