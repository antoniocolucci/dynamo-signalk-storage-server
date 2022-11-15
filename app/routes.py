import os
import tempfile

from flask_restx import Api, Resource
from flask import current_app, send_file, render_template, request
from sqlalchemy import create_engine
from werkzeug.datastructures import FileStorage

from app import app, auth, get_conf, log, queues

# Create the api object using restx
api = Api(app)


@api.route('/publickey')
class PublicKey(Resource):
    def get(self):
        conf = get_conf()
        log.debug("public_key_filename:"+conf["public_key_filename"])
        return send_file(conf["public_key_filename"], as_attachment=True)


@auth.verify_password
def authenticate(username, password):
    if username and password:
        if username == 'admin' and password == 'password':
            return True
        else:
            return False
    return False


# Create an api parser for public key upload
upload_publickey_parser = api.parser()

# Add parser for file storage
upload_publickey_parser.add_argument('file', location='files', type=FileStorage, required=True,
                                     help='DYNAMO Public Key -public.pem file')


@api.route('/upload/publickey/<self_id>')
@api.expect(upload_publickey_parser)
class PublicKeyUpload(Resource):
    @auth.login_required
    def post(self, self_id):
        # This is FileStorage instance
        uploaded_file = request.files['file']

        # Check if the mime is plain/text
        if uploaded_file.mimetype == 'application/x-x509-ca-cert':

            # Compose the destination file path name
            file_path = os.path.join(current_app.config.get('PUBLIC_KEY_ROOT'), str(self_id) + "-public.pem")

            # Save the uploaded file as the file path
            uploaded_file.save(file_path)

            # Log the status
            log.debug("Saved public key as: " + file_path)

            return {"result": "ok", "user": auth.current_user()}, 200

        else:
            return {"result": "fail", "error": "File mimetype must be application/x-x509-ca-cert"}, 422


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

            return {'result': 'ok'}, 200
        else:
            return {'result': 'fail', "error": "File mimetype must be application/octet-stream"}, 422


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
                "SELECT ctx.context, pos.timestamp, ctx.value as info, pos.position FROM public.context ctx, "
                "(SELECT context, timestamp, value as position FROM public.navigation_position np1 WHERE timestamp = "
                "(SELECT MAX(np2.timestamp) FROM public.navigation_position np2 WHERE np1.context = np2.context) "
                "ORDER BY context) pos WHERE ctx.context = pos.context AND ctx.timestamp = pos.timestamp;"
            )
            for row in result:
                positions.append({
                    "id": row[0].split(":")[-1],
                    "timestamp": str(row[1]),
                    "info": row[2],
                    "position": row[3]
                })
            conn.close()
        except:
            conn.close()

        return positions


@app.route('/map')
def index():
    return render_template('index.html')
