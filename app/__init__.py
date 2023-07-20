import logging
import os
import time
import json

from flask_restx import Api
from sqlalchemy import create_engine, MetaData
from celery import Celery
from celery import Task
from flask import Flask

from os import listdir
from os.path import isdir, isfile, join

from flask_cors import CORS

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

flask_app = None


def create_app() -> Flask:
    app_root = os.getcwd()

    # Log a debug message
    log.debug("App root:" + app_root)

    # Create the Flask application
    app = Flask(__name__)

    # Set the api enabled for cross server invocation
    CORS(app)

    # Check if the file config.json exists
    if not isfile(app_root + os.sep + 'config.json'):
        log.error("Missing config.json file"),
        exit(-1)

    # Configure the application from the config file
    app.config.from_file(app_root + os.sep + "config.json", load=json.load)

    # Override config.json configuration with environmental variables
    app.config.from_prefixed_env()

    # Log a debug message
    log.debug("app.config: " + str(app.config))

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

        # Open the private key file
        f = open(app.config['PRIVATE_KEY_FILENAME'], "r")

        # Read the private key
        private_key_pem = f.read()

        # Close the file
        f.close()

        # Import the private key
        imported_private_key = RSA.importKey(private_key_pem)

        # Construct the imported private key
        private_key = RSA.construct(
            (
                imported_private_key.n,
                imported_private_key.e,
                imported_private_key.d,
                imported_private_key.p,
                imported_private_key.q
            )
        )

        # Export the public key from the private key
        public_key = private_key.publickey().export_key()

        # Create a file for the private key
        f = open(app.config['PUBLIC_KEY_FILENAME'], "w")

        # Write the private key on a file
        f.write(public_key.decode())

        # Close the file
        f.close()

        # Log info message
        log.info("Created public key: " + app.config['PUBLIC_KEY_FILENAME'])

    # Check the public key directory root
    if not isdir(app.config['PUBLIC_KEY_ROOT']):
        # Log the info message
        log.info("Public key directory root not found")

        # Create the directory
        os.mkdir(app.config['PUBLIC_KEY_ROOT'])

        # Log info message
        log.info("Created public key directory root: " + app.config['PUBLIC_KEY_ROOT'])

    # Check if the database backend is up and running

    # Connect the database server
    engine = create_engine(app.config["CONNECTION_STRING"], echo=False)

    # Retrieve the metadata
    metadata = MetaData()

    try:
        # Create the actual connection
        metadata.create_all(engine)

    except Exception as exception:
        # Log the error message
        log.error("Database connection error. Check the connection string: " +
                  app.config["CONNECTION_STRING"] + ": " +
                  str(exception))

        # Exit with error code
        exit(-1)

    # Log the info message
    log.info("Private key: " + app.config['PRIVATE_KEY_FILENAME'])
    log.info("Public key: " + app.config['PUBLIC_KEY_FILENAME'])
    log.info("Public key directory root: " + app.config['PUBLIC_KEY_ROOT'])
    log.info("Database connection string: " + app.config['CONNECTION_STRING'])

    # Initialize the Celery app
    celery_init_app(app)

    # Process the files in the media folder
    def process_files(config):
        # Import the process file
        from app.tasks import process_file_task

        # Log the info message
        log.info("Start " + time.strftime("%A, %d. %B %Y %I:%M:%S %p") + "...")

        # Get the media root path
        media_root = config["MEDIA_ROOT"]

        # Get all directories in the media root (each directory is a vessel uuid)
        directories = [f for f in listdir(media_root) if isdir(join(media_root, f))]

        # For each directory in the directory list...
        for directory in directories:

            # Create the directory path
            directory_root = media_root + "/" + directory

            # Create the file list. Each file contains a data parcel.
            files = [f for f in listdir(directory_root) if isfile(join(directory_root, f))]

            # Fir each file in the file list
            for file_item in files:
                # Log an info message
                log.info("Processing:" + media_root + "," + directory + "," + file_item)

                # Processing the file is potentially time-consuming, enqueue the process
                process_file_task.delay(directory, file_item)

        # Log an info message
        log.info("... " + time.strftime("%A, %d. %B %Y %I:%M:%S %p") + " finish.")

    with app.app_context():
        from . import routes

    # Process files still to be processed
    process_files(app.config)

    return app


def celery_init_app(app: Flask) -> Celery:
    class FlaskTask(Task):
        def __call__(self, *args: object, **kwargs: object) -> object:
            with app.app_context():
                return self.run(*args, **kwargs)

    celery_app = Celery(
        app.name,
        task_cls=FlaskTask
    )
    celery_app.config_from_object(app.config["CELERY"])
    celery_app.set_default()
    app.extensions["celery"] = celery_app
    return celery_app
