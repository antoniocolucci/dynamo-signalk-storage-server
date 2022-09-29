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


queues = Queues(process_file_queue, 8)
queues.start()


def get_conf():
    media_root = app.config.get("MEDIA_ROOT")
    scratch_root = app.config.get("SCRATCH_ROOT")
    trash_root = app.config.get("TRASH_ROOT")

    connection_string = app.config.get("CONNECTION_STRING")

    private_key_filename = app.config.get("PRIVATE_KEY_FILENAME")
    public_key_root = app.config.get("PUBLIC_KEY_ROOT")

    conf = {
        "media_root": media_root,
        "scratch_root": scratch_root,
        "trash_root": trash_root,
        "connection_string": connection_string,
        "private_key_filename": private_key_filename,
        "public_key_root": public_key_root
    }

    return conf


def process_files():
    log.info("Start " + time.strftime("%A, %d. %B %Y %I:%M:%S %p") + "...")

    conf = get_conf()
    media_root = conf["media_root"]

    vessels = [f for f in listdir(media_root) if isdir(join(media_root, f))]
    for vessel in vessels:
        vessel_root = media_root + "/" + vessel
        files = [f for f in listdir(vessel_root) if isfile(join(vessel_root, f))]
        for file_item in files:
            log.info("Processing:" + media_root + "," + vessel + "," + file_item)
            # process_file_task.delay(vessel, file_item, conf)
            queues.enqueue({"vessel": vessel, "file_item": file_item, "conf": conf})

    queues.join()
    log.info("... " + time.strftime("%A, %d. %B %Y %I:%M:%S %p") + " finish.")


api = Api(app)
process_files()


@api.route('/publickey')
class public_key(Resource):
    def get(self):
        return send_file("keys/dynamo-store-public.pem", as_attachment=True)


@api.route('/upload/<selfId>')
class my_file_upload(Resource):
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
            queues.enqueue({"vessel": selfId, "file_item": temp_name + '.log.gz.enc', "conf": conf})
        else:
            return {"error": "File mimetype must be application/octet-stream"}, 422

        return {'status': 'Done'}


@api.route('/lastPosition')
class last_position(Resource):
    def get(self):
        conf = get_conf()

        connection_string = conf["connection_string"]
        engine = create_engine(connection_string, echo=False)
        conn = engine.connect()

        result = conn.execute("SELECT context, timestamp, value FROM public.navigation_position np1 WHERE timestamp = ( SELECT MAX( np2.timestamp ) FROM public.navigation_position np2 WHERE np1.context = np2.context ) ORDER BY context;")
        positions = []
        for row in result:
            positions.append({
                "id": row[0].split(":")[-1],
                "timestamp": str(row[1]),
                "value": row[2]
            })
        conn.close()
        return positions


@app.route('/map')
def index():
    return render_template('index.html')


if __name__ == '__main__':
    app.run(use_reloader=False)