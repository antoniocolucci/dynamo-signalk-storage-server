from flask import Flask, Blueprint, url_for, jsonify, current_app, abort, send_from_directory
from flask_restplus import Api, Resource, reqparse
from flask_marshmallow import Marshmallow, base_fields
from marshmallow import post_dump
import werkzeug
import tempfile

import time
import atexit


from apscheduler.schedulers.background import BackgroundScheduler

import sys, os, shutil
from os import listdir
from os.path import isdir, isfile, join

from uncompress import get_encoded_encrypted_symmetric_key, get_symmetric_key, uncrypt_update_list
from store import store_updatelist_csv, store_updatelist_odbc

import logging

log = logging.getLogger('apscheduler.executors.default')
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



class BaseConfig(object):
    DATA_FOLDER="/data2/ccmmma/prometeo/data/dynamo-store/data_folder"
    
    MEDIA_ROOT   = DATA_FOLDER+"/media/"
    SCRATCH_ROOT = DATA_FOLDER+"/scratch/"
    TRASH_ROOT   = DATA_FOLDER+"/trash/"
    CSV_ROOT     = DATA_FOLDER+"/csv/"

    CONNECTION_STRING = "DRIVER=SQLite3;SERVER=localhost;DATABASE="+DATA_FOLDER+"/dynamo.db;Trusted_connection=yes"

    PRIVATE_KEY_FILENAME = "/home/ccmmma/dev/dynamo-store/keys/dynamo-store-private.pem"
    PUBLIC_KEY_ROOT      = "/home/ccmmma/dev/dynamo-store/keys/public"


config = {
    "default": BaseConfig
}


def configure_app(app):
    config_name = os.getenv('FLASK_CONFIGURATION', 'default')
    app.config.from_object(config[config_name]) # object-based default configuration
    app.config.from_pyfile('config.cfg', silent=True) # instance-folders configuration



configure_app(app)

scheduler = BackgroundScheduler()
scheduler.start()

def heartbeat():
    log.info("Start "+time.strftime("%A, %d. %B %Y %I:%M:%S %p")+"...")
    job.pause()
    log.debug("Paused")

    media_root = app.config.get("MEDIA_ROOT")
    scratch_root = app.config.get("SCRATCH_ROOT")
    trash_root = app.config.get("TRASH_ROOT")
    csv_root = app.config.get("CSV_ROOT")
    connection_string = app.config.get("CONNECTION_STRING")

    private_key_filename = app.config.get("PRIVATE_KEY_FILENAME")
    public_key_root = app.config.get("PUBLIC_KEY_ROOT")

    vessels = [f for f in listdir(media_root) if isdir(join(media_root, f))]
    for vessel in vessels:
        public_key_filename=public_key_root+"/"+vessel+"-public.pem"
        vessel_root=media_root+"/"+vessel
        files = [f for f in listdir(vessel_root) if isfile(join(vessel_root, f))]
        for file in files:
            src_path=vessel_root+"/"+file
            log.info("Processing: "+src_path)
            try:
                os.mkdir(scratch_root+"/"+vessel)
            except:
                pass

            scratch_dir=tempfile.mkdtemp(dir=scratch_root+"/"+vessel)
            enc_path = scratch_dir + "/" + file

            try:
                log.debug("Get Eencoded Encrypted Symmetric Key")
                encoded_encrypted_symmetric_key=get_encoded_encrypted_symmetric_key(src_path,enc_path)

                log.debug("Get Symmetric Key")
                symmetric_key=get_symmetric_key(private_key_filename, encoded_encrypted_symmetric_key)

                log.debug("Uncrypt Update List")
                update_list=uncrypt_update_list(public_key_filename, symmetric_key, enc_path)
            
                #log.debug("Store Update List CSV")
                #store_updatelist_csv(update_list, { "csv_root": csv_root} )

                log.debug("Store Update List ODBC")
                store_updatelist_odbc(update_list, { "connection_string": connection_string} )
            except ValueError as e:
                log.error("Exception:", e)

            shutil.rmtree(scratch_dir)

            try:
                os.mkdir(trash_root+"/"+vessel)
            except:
                pass

            os.rename(src_path, trash_root + "/"+vessel + "/" + file )

    log.debug("Resume")
    job.resume()
    log.info("...finish.")

job=scheduler.add_job(func=heartbeat, trigger="interval", seconds=60)

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())

api = Api(app)

@api.route('/publickey')
class public_key(Resource):
    def get(self):
        return send_from_directory('keys', "dynamo-store-public.pem", as_attachment=True)

@api.route('/upload/<selfId>')
class my_file_upload(Resource):
    @api.expect(file_upload)
    def post(self,selfId):

        args = file_upload.parse_args()
        log.debug(args['file'].mimetype)
        if args['file'].mimetype == 'application/octet-stream':
            destination = os.path.join(current_app.config.get('DATA_FOLDER'), 'media/'+str(selfId)+"/")
            if not os.path.exists(destination):
                os.makedirs(destination)

            done=False
            while not done:
                temp_name = next(tempfile._get_candidate_names())
                file_path = '%s%s%s' % (destination,temp_name, '.log.gz.enc')
                if os.path.isfile(file_path) is False:
                    done=True
            log.debug("Path: "+str(file_path))

            args['file'].save(file_path)
        else:
            abort(404)
        return {'status': 'Done'}




if __name__ == '__main__':
    app.run(use_reloader=False)
