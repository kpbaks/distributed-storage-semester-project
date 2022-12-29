"""
Aarhus University - Distributed Storage course - Lab 4

REST Server, starter template for Week 4
"""

from flask import Flask, make_response, g, request, send_file
import sqlite3
import base64
import zmq # For ZMQ
import time # For waiting a second for ZMQ connections
import math # For cutting the file in half
import random # For selecting a random half when requesting chunks
import messages_pb2 # Generated Protobuf messages
import io # For sending binary data in a HTTP response
import logging
import raid1
import json 
import rlnc
import speedtest


# Initiate ZMQ sockets
context = zmq.Context()
# Socket to send tasks to Storage Nodes
send_task_socket = context.socket(zmq.PUSH)
send_task_socket.bind("tcp://*:5557")
# Socket to receive messages from Storage Nodes
response_socket = context.socket(zmq.PULL)
response_socket.bind("tcp://*:5558")
# Publisher socket for data request broadcasts
data_req_socket = context.socket(zmq.PUB)
data_req_socket.bind("tcp://*:5559")
# Wait for all workers to start and connect.
time.sleep(1)
print("Listening to ZMQ messages on tcp://*:5558")
"""
Utility Functions
"""

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(
            'files.db',
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

    return g.db

def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()

def random_string(length=8):
    """
    Returns a random alphanumeric string of the given length. 
    Only lowercase ascii letters and numbers are used.

    :param length: Length of the requested random string 
    :return: The random generated string
    """
    import random, string
    return ''.join([random.SystemRandom().choice(string.ascii_letters + string.digits) for n in range(length)])

def write_file(data, filename=None):
    """
    Write the given data to a local file with the given filename

    :param data: A bytes object that stores the file contents
    :param filename: The file name. If not given, a random string is generated
    :return: The file name of the newly written file, or None if there was an error
    """
    if not filename:
        # Generate random filename
        filename = random_string(length=8)
        # Add '.bin' extension
        filename += ".bin"
    
    try:
        # Open filename for writing binary content ('wb')
        # note: when a file is opened using the 'with' statment, 
        # it is closed automatically when the scope ends
        with open('./'+filename, 'wb') as f:
            f.write(data)
    except EnvironmentError as e:
        print("Error writing file: {}".format(e))
        return None
    
    return filename
#



"""
REST API
"""

# Instantiate the Flask app (must be before the endpoint functions)
app = Flask(__name__)
# Close the DB connection after serving the request
app.teardown_appcontext(close_db)

@app.route('/')
def hello():
    return make_response({'message': 'Hello World!'})

@app.route('/files',  methods=['GET'])
def list_files():
    db = get_db()
    cursor = db.execute("SELECT * FROM `file`")
    if not cursor: 
        return make_response({"message": "Error connecting to the database"}, 500)
    
    files = cursor.fetchall()
    # Convert files from sqlite3.Row object (which is not JSON-encodable) to 
    # a standard Python dictionary simply by casting
    files = [dict(file) for file in files]
    
    return make_response({"files": files})
#

@app.route('/files/<int:file_id>',  methods=['GET'])
def download_file(file_id):

    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
    if not cursor: 
        return make_response({"message": "Error connecting to the database"}, 500)
    
    f = cursor.fetchone()
    if not f:
        return make_response({"message": "File {} not found".format(file_id)}, 404)

    # Convert to a Python dictionary
    f = dict(f)
    print("File requested: {}".format(f['filename']))
    
    # Parse the storage details JSON string
    import json
    storage_details = json.loads(f['storage_details'])

    if f['storage_mode'] == 'raid1':
        
        part1_filenames = storage_details['part1_filenames']
        #part2_filenames = storage_details['part2_filenames']

        file_data = raid1.get_file(
            part1_filenames, 
            #part2_filenames, 
            data_req_socket, 
            response_socket
        )

    elif f['storage_mode'] == 'erasure_coding_rs':
        
        coded_fragments = storage_details['coded_fragments']
        max_erasures = storage_details['max_erasures']

        pass
        
    elif f['storage_mode'] == 'erasure_coding_rlnc':
        
        coded_fragments = storage_details['coded_fragments']
        max_erasures = storage_details['max_erasures']

        pass

    return send_file(io.BytesIO(file_data), mimetype=f['content_type'])
#

# HTTP HEAD requests are served by the GET endpoint of the same URL,
# so we'll introduce a new endpoint URL for requesting file metadata.
@app.route('/files/<int:file_id>/info',  methods=['GET'])
def get_file_metadata(file_id):

    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
    if not cursor: 
        return make_response({"message": "Error connecting to the database"}, 500)
    
    f = cursor.fetchone()
    if not f:
        return make_response({"message": "File {} not found".format(file_id)}, 404)

    # Convert to a Python dictionary
    f = dict(f)
    print("File: %s" % f)

    return make_response(f)
#

@app.route('/files/<int:file_id>',  methods=['DELETE'])
def delete_file(file_id):

    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
    if not cursor: 
        return make_response({"message": "Error connecting to the database"}, 500)
    
    f = cursor.fetchone()
    if not f:
        return make_response({"message": "File {} not found".format(file_id)}, 404)

    # Convert to a Python dictionary
    f = dict(f)
    print("File to delete: %s" % f)

    # TODO Delete all chunks from the Storage Nodes

    # TODO Delete the file record from the DB

    # Return empty 200 Ok response
    return make_response('TODO: implement this endpoint', 404)
#

@app.route('/files_mp', methods=['POST'])
def add_files_multipart():
    # Flask separates files from the other form fields
    payload = request.form
    files = request.files
    
    # Make sure there is a file in the request
    if not files or not files.get('file'):
        logging.error("No file was uploaded in the request!")
        return make_response("File missing!", 400)
    
    # Reference to the file under 'file' key
    file = files.get('file')
    # The sender encodes a the file name and type together with the file contents
    filename = file.filename
    content_type = file.mimetype
    # Load the file contents into a bytearray and measure its size
    data = bytearray(file.read())
    size = len(data)
    print("File received: %s, size: %d bytes, type: %s" % (filename, size, content_type))
    
    # Read the requested storage mode from the form (default value: 'raid1')
    storage_mode = payload.get('storage', 'raid1')
    print("Storage mode: %s" % storage_mode)

    if storage_mode == 'raid1':
        file_data_1_names, file_data_2_names = raid1.store_file(data, send_task_socket, response_socket)

        storage_details = {
            "part1_filenames": file_data_1_names,
            "part2_filenames": file_data_2_names
        }

    elif storage_mode == 'erasure_coding_rs':
        # Reed Solomon code
        # Parse max_erasures (everything is a string in request.form, 
        # we need to convert to int manually), set default value to 1
        max_erasures = int(payload.get('max_erasures', 1))
        print("Max erasures: %d" % (max_erasures))
        
        pass

    elif storage_mode == 'erasure_coding_rlnc':
        # RLNC
        max_erasures = int(payload.get('max_erasures', 1))
        print("Max erasures: %d" % (max_erasures))

        pass
        
    else:
        logging.error("Unexpected storage mode: %s" % storage_mode)
        return make_response("Wrong storage mode", 400)

    # Insert the File record in the DB
    import json
    db = get_db()
    cursor = db.execute(
        "INSERT INTO `file`(`filename`, `size`, `content_type`, `storage_mode`, `storage_details`) VALUES (?,?,?,?,?)",
        (filename, size, content_type, storage_mode, json.dumps(storage_details))
    )
    db.commit()

    return make_response({"id": cursor.lastrowid }, 201)
#

@app.route('/files', methods=['POST'])
def add_files():
    payload = request.get_json()
    filename = payload.get('filename')
    content_type = payload.get('content_type')
    file_data = base64.b64decode(payload.get('contents_b64'))
    size = len(file_data)

#TODO change the implementation of the database to one file imageine the chunks as one file
#FIXME the file_data_1_names and file_data_2_names are not used and keep them as 1 file


    #file_data_1_names, file_data_2_names = raid1.store_file(file_data, send_task_socket, response_socket)
    
    #write cases for the 4 files
    #First get the arguments from terminal

    # Measure the bandwidth of the Internet connection
    

    import sys
    if len(sys.argv) >= 2:
    # Print the second command-line argument
        print(sys.argv[1])
        if sys.argv[1] == "1":
                file_data_1_names = raid1.store_file(file_data, send_task_socket, response_socket)
                db = get_db()
                cursor = db.execute(
                "INSERT INTO `file`(`filename`, `size`, `content_type`, `part1_filenames`) VALUES (?,?,?,?)",
                (filename, size, content_type, ','.join(file_data_1_names))
                )
                db.commit()
                
        elif sys.argv[1] >= "2":
            #put sys.argv[1] as a range in the for loop
            
            replica = int(sys.argv[1])
            for i in range(0, replica):
                file_data_1_names = raid1.store_file(file_data, send_task_socket, response_socket)
                db = get_db()
                cursor = db.execute(
                "INSERT INTO `file`(`filename`, `size`, `content_type`, `part1_filenames`) VALUES (?,?,?,?)",
                (filename, size, content_type, ','.join(file_data_1_names))
                )
                db.commit() 

                
     # Insert the File record in the DB
    
   

    # Return the ID of the new file record with HTTP 201 (Created) status code
    return make_response({"id": cursor.lastrowid }, 201)
#
@app.errorhandler(500)
def server_error(e):
    logging.exception("Internal error: %s", e)
    return make_response({"error": str(e)}, 500)



# Start the Flask app (must be after the endpoint functions) 
host_local_computer = "localhost" # Listen for connections on the local computer
host_local_network = "0.0.0.0" # Listen for connections on the local network
app.run(host=host_local_computer, port=9000)