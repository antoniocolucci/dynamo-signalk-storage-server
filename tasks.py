import tempfile

import celery

import os, shutil
import logging

from uncompress import get_encoded_encrypted_symmetric_key, get_symmetric_key, uncrypt_update_list
from storage import store_updatelist

log = logging.getLogger('tasks')
log.setLevel(logging.DEBUG)  # DEBUG

fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
h = logging.StreamHandler()
h.setFormatter(fmt)
log.addHandler(h)


def process_file_queue(args):
    process_file_task(args["directory"], args["file_item"], args["conf"])


@celery.task
def process_file_task(directory, file_item, conf):

    # Retrieve the media root (where the flask route saves the "type.context/...log.gz.enc" files
    media_root = conf["media_root"]

    # Retrieve the scratch root (where the "type.context/...log.gz.enc" files are unzipped, decrypted and processed.
    scratch_root = conf["scratch_root"]

    # Retrieve the trash root (where the "type.context/...log.gz.enc" files are stored before removed forever)
    trash_root = conf["trash_root"]

    # Get the connection string
    connection_string = conf["connection_string"]

    # Get the local private key file name
    private_key_filename = conf["private_key_filename"]

    # Check if the local private key exists
    if os.path.isfile(private_key_filename):

        # Get the public key root (where the public keys are stored in the format type.context-public.pem
        public_key_root = conf["public_key_root"]

        # Set the public key file name
        public_key_filename = public_key_root + "/" + directory + "-public.pem"

        # Check if the file exists
        if os.path.isfile(public_key_filename):

            src_path = media_root + "/" + directory + "/" + file_item

            log.info("Processing: " + src_path)
            try:
                os.makedirs(scratch_root + "/" + directory)
            except FileExistsError as e:
                pass

            scratch_dir = tempfile.mkdtemp(dir=scratch_root + "/" + directory)
            enc_path = scratch_dir + "/" + file_item

            try:
                log.debug("Get Encoded Encrypted Symmetric Key")
                encoded_encrypted_symmetric_key = get_encoded_encrypted_symmetric_key(src_path, enc_path)

                log.debug("Get Symmetric Key")
                symmetric_key = get_symmetric_key(private_key_filename, encoded_encrypted_symmetric_key)

                log.debug("Decrypt the Update List")
                update_list = uncrypt_update_list(public_key_filename, symmetric_key, enc_path)

                log.debug("Store the Update List")
                store_updatelist(update_list, {"connection_string": connection_string})

                shutil.rmtree(scratch_dir)

            except Exception as exception:
                log.error(exception)
            try:
                os.makedirs(trash_root + "/" + directory)
            except FileExistsError as e:
                pass

            os.rename(src_path, trash_root + "/" + directory + "/" + file_item)
        else:
            log.debug("Public key file missing: " + public_key_filename)
    else:
        log.debug("Private key file missing: " + private_key_filename)
