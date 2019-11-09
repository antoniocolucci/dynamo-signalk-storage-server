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
    process_file_task(args["vessel"], args["file_item"], args["conf"])


@celery.task
def process_file_task(vessel, file_item, conf):
    media_root = conf["media_root"]
    scratch_root = conf["scratch_root"]
    trash_root = conf["trash_root"]

    connection_string = conf["connection_string"]

    private_key_filename = conf["private_key_filename"]
    public_key_root = conf["public_key_root"]

    public_key_filename = public_key_root + "/" + vessel + "-public.pem"

    src_path = media_root+"/"+vessel+"/"+file_item

    log.info("Processing: " + src_path)
    try:
        os.mkdir(scratch_root + "/" + vessel)
    except:
        pass

    scratch_dir = tempfile.mkdtemp(dir=scratch_root + "/" + vessel)
    enc_path = scratch_dir + "/" + file_item

    try:
        log.debug("Get Eencoded Encrypted Symmetric Key")
        encoded_encrypted_symmetric_key = get_encoded_encrypted_symmetric_key(src_path, enc_path)

        log.debug("Get Symmetric Key")
        symmetric_key = get_symmetric_key(private_key_filename, encoded_encrypted_symmetric_key)

        log.debug("Uncrypt Update List")
        update_list = uncrypt_update_list(public_key_filename, symmetric_key, enc_path)

        log.debug("Store Update List")
        store_updatelist(update_list, {"connection_string": connection_string})
    except ValueError as e:
        log.error("Exception:", e)

    shutil.rmtree(scratch_dir)

    try:
        os.mkdir(trash_root + "/" + vessel)
    except:
        pass

    os.rename(src_path, trash_root + "/" + vessel + "/" + file_item)
