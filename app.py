import logging
import os
import tempfile
import time

from celery import Celery

from werkzeug.datastructures import FileStorage

from flask import Flask, current_app, send_file, render_template, request
from flask_restx import Api, Resource
from os import listdir
from os.path import isdir, isfile, join
from sqlalchemy import create_engine
from flask_cors import CORS

from queues import Queues
from tasks import process_file_queue

log = logging.getLogger('app')
log.setLevel(logging.DEBUG)  # DEBUG

fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
h = logging.StreamHandler()
h.setFormatter(fmt)
log.addHandler(h)

app = Flask(__name__)
CORS(app)

app.config.from_pyfile('config.cfg', silent=True)  # instance-folders configuration

celery_app = Celery(app.name, backend=app.config['CELERY_RESULT_BACKEND'], broker=app.config['CELERY_BROKER_URL'])
celery_app.conf.update(app.config)


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
queues = Queues(process_file_queue, get_conf()["queue_concurrency"])

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


# Create the api object using restx
api = Api(app)

# Process files still to be processed
process_files()


@api.route('/publickey')
class PublicKey(Resource):
    def get(self):
        conf = get_conf()
        return send_file(conf["public_key_filename"], as_attachment=True)

# Create an api parser
upload_parser = api.parser()

# Add parser for file storage
upload_parser.add_argument('file', location='files', type=FileStorage, required=True, help='DYNAMO log.gz.enc file')

# Add parser for session id
upload_parser.add_argument('sessionId', required=True, help="Session unique identifier")


@api.route('/upload/<self_id>')
@api.expect(upload_parser)
class ParcelUpload(Resource):
    def post(self, self_id):

        # This is FileStorage instance
        uploaded_file = request.files['file']

        # Check if the myme is application/octet-stream
        if uploaded_file.mimetype == 'application/octet-stream':

            # Compose the destination directory
            destination = os.path.join(current_app.config.get('MEDIA_ROOT'), str(self_id) + "/")

            # Check if the directory exists
            if not os.path.exists(destination):

                # Make the directory if needed
                os.makedirs(destination)

            # Set the file path
            file_path = ""

            # Set the file name
            temp_name = ""

            # Until the random temporary file name is not unique...
            done = False
            while not done:

                # Generate a temporary file name
                temp_name = next(tempfile._get_candidate_names())

                # Create a file name
                file_path = '%s%s%s' % (destination, temp_name, '.log.gz.enc')

                # Check if the file already exists
                if os.path.isfile(file_path) is False:

                    # If the file doesn't exist, exit the cycle
                    done = True

            # Save the uploaded file as the file path
            uploaded_file.save(file_path)

            # Enqueue the task
            queues.enqueue({"directory": self_id, "file_item": temp_name + '.log.gz.enc', "conf": get_conf()})
        else:
            return {"error": "File mimetype must be application/octet-stream"}, 422

        return {'status': 'Done'}, 200


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
