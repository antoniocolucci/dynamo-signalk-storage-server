import logging
import os
import time

from sqlalchemy import create_engine, Column, Table, JSON, Text, Float, MetaData, DateTime, select
from geoalchemy2 import Geometry

from celery import Celery

from flask import Flask

from flask_httpauth import HTTPBasicAuth
from os import listdir
from os.path import isdir, isfile, join

from flask_cors import CORS

from app.queues import Queues

from base64 import b64decode
from Crypto.PublicKey import RSA  # provided by pycryptodome

# Create the logger
log = logging.getLogger('app')

# Set the default logger level as debug
log.setLevel(logging.DEBUG)

# Create the logger formatter
fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

# Get the handler
h = logging.StreamHandler()

# Set the formatter
h.setFormatter(fmt)

# Add the handler to the logger
log.addHandler(h)

# Create the Flask application
app = Flask(__name__)

# Set the api enabled for cross server invocation
CORS(app)

# Check if the file config.cfg exists
if not isfile('./config.cfg'):
    log.error("Missing config.cfg file"),
    exit(-1)

# Configure the application from the config file
app.config.from_pyfile('../config.cfg', silent=False)

# Check the private key
if not isfile(app.config['PRIVATE_KEY_FILENAME']):
    # Log the info message
    log.info("Private key not found")

    # Generate an RSA public key
    key = RSA.generate(2048)

    # Get the private key
    private_key = key.export_key()

    # Create a file for the private key
    f = open(app.config['PRIVATE_KEY_FILENAME'], "w")

    # Write the private key on a file
    f.write(private_key.decode())

    # Close the file
    f.close()

    log.info("Created private key: " + app.config['PRIVATE_KEY_FILENAME'])

# Check the public key
if not isfile(app.config['PUBLIC_KEY_FILENAME']):
    # Log the info message
    log.info("Public key not found")

    # Read the private key
    f = open(app.config['PRIVATE_KEY_FILENAME'], "r")
    private_key_pem = f.read()

    f.close()

    imported_private_key = RSA.importKey(private_key_pem)

    private_key = RSA.construct(
        (
            imported_private_key.n,
            imported_private_key.e,
            imported_private_key.d,
            imported_private_key.p,
            imported_private_key.q
        )
    )

    public_key = private_key.publickey().export_key()

    # Create a file for the private key
    f = open(app.config['PUBLIC_KEY_FILENAME'], "w")

    # Write the private key on a file
    f.write(public_key.decode())

    # Close the file
    f.close()

    # Log info message
    log.info("Created public key: " + app.config['PUBLIC_KEY_FILENAME'])

# CHeck the public key directory root
if not isdir(app.config['PUBLIC_KEY_ROOT']):
    # Log the info message
    log.info("Public key directory root not found")

    os.mkdir(app.config['PUBLIC_KEY_ROOT'])

    # Log info message
    log.info("Created public key directory root: " + app.config['PUBLIC_KEY_ROOT'])

# Check the database backend

# Connect the database server
engine = create_engine(app.config["CONNECTION_STRING"], echo=False)

# Retrieve the metadata
metadata = MetaData()

try:
    metadata.create_all(engine)

except:
    log.error("Database connection error. Check the connection string: " + app.config["CONNECTION_STRING"])
    exit(-1)

# Log the info message
log.info("Private key: " + app.config['PRIVATE_KEY_FILENAME'])
log.info("Public key: " + app.config['PUBLIC_KEY_FILENAME'])
log.info("Public key directory root: " + app.config['PUBLIC_KEY_ROOT'])
log.info("Database connection string: " + app.config['CONNECTION_STRING'])

# Get the HTTP Basic Authentication object
auth = HTTPBasicAuth()

# Create the celery application
celery_app = Celery(app.name, backend=app.config['CELERY_RESULT_BACKEND'], broker=app.config['CELERY_BROKER_URL'])

# Set the celery configuration as the flask application
celery_app.conf.update(app.config)

# Define a worker ToDo: Check if this function is really used
# def myworker(queue_item):
#    print("myworker:" + queue_item)


from app.tasks import process_file_queue


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


# Process files still to be processed
process_files()

from app import routes
