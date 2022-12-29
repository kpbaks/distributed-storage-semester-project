"""
Aarhus University - Distributed Storage course - Lab 6

REST API + RAID Controller
"""
import argparse
import atexit  # unregister scheduler at app exit
import base64
import io  # For sending binary data in a HTTP response
import logging
import math  # For cutting the file in half
import os
import random
import kodo
import sqlite3
import string
import sys
import time  # For waiting a second for ZMQ connections
import json
from pprint import pp
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from utils import random_string

from hashlib import sha256

import itertools as it

import zmq  # For ZMQ
# from apscheduler.schedulers.background import \
#     BackgroundScheduler  # automated repair
from flask import Flask, Response, g, make_response, request, send_file

import messages_pb2  # Generated Protobuf messages
import reedsolomon

from reedsolomon import ReedSolomonStorageProvider
from utils import is_raspberry_pi, flatten_list

STORAGE_NODES_NUM = 4

def get_db(filename: str = "files.db") -> sqlite3.Connection:
    """Get a database connection. Create it if it doesn't exist."""
    if (
        "db" not in g
    ):  # g is a special object that is unique for each request, and can be used to store data.
        g.db = sqlite3.connect(filename, detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row

    return g.db


def close_db(e=None) -> None:
    db = g.pop("db", None)

    if db is not None:
        db.close()

logger = logging.getLogger(__name__)
YELLOW = "\033[93m"
NC = "\033[0m"  # No Color
# formatter = logging.Formatter('[%(levelname)s: %(asctime)s](%(name)s) - %(message)s')
format: str = f"[{YELLOW}%(levelname)s{NC}] (%(name)s) - %(message)s"

if os.environ.get("DEBUG"):
    logging.basicConfig(level=logging.DEBUG, format=format)
else:
    logging.basicConfig(level=logging.INFO, format=format)

logger.info(f"log level is {logger.getEffectiveLevel()}")

logger.info("Initializing ZMG sockets ...")
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

# Publisher socket for fragment repair broadcasts
repair_socket = context.socket(zmq.PUB)
repair_socket.bind("tcp://*:5560")

# Socket to receive repair messages from Storage Nodes
repair_response_socket = context.socket(zmq.PULL)
repair_response_socket.bind("tcp://*:5561")

# Publisher socket for fragment repair broadcasts
del_socket = context.socket(zmq.PUB)
del_socket.bind("tcp://*:5565")

# Socket to receive repair messages from Storage Nodes
del_response_socket = context.socket(zmq.PULL)
del_response_socket.bind("tcp://*:5566")

logger.info("Initializing ZMG sockets ... DONE")
# Wait for all workers to start and connect.
time.sleep(1)
logger.info("Listening to ZMQ messages on tcp://*:5558 and tcp://*:5561")

logger.debug("Instantiating Flask app")
# Instantiate the Flask app (must be before the endpoint functions)
app = Flask(__name__)
# Close the DB connection after serving the request
app.teardown_appcontext(close_db)

# -----------------------------------------------
# TASK HELPERS
# -----------------------------------------------
node_ids = ["KLZl27hY","VHmKBHH1", "4znGZO5I","Lrv9QGzI"]

def send_file_to_node(node_id, file_data, task, send_task_socket, response_socket):

    # Create a header message
    header = messages_pb2.header()
    header.request_type = messages_pb2.STORE_FRAGMENT_DATA_REQ

    # Send the file to the other node
    send_task_socket.send_multipart([node_id.encode('UTF-8'),
                                     header.SerializeToString(),
                                     task.SerializeToString(),
                                     file_data])

    # Wait for a response from the other node
    resp = response_socket.recv_string()
    return resp

def insert_into_db(filename, size, content_type, storage_mode, storage_details):
    
    # Insert the File record in the DB
    db = get_db()
    cursor = db.execute(
        "INSERT INTO `file`(`filename`, `size`, `content_type`, `storage_mode`, `storage_details`) VALUES (?,?,?,?,?)",
        (filename, size, content_type, storage_mode, json.dumps(storage_details))
    )
    db.commit()

    return cursor

def get_file(filename, storage_mode, data_req_socket, response_socket):
    task1 = messages_pb2.getdata_request()
    task1.filename = filename
    task1.storage_mode = storage_mode
    data_req_socket.send(
        task1.SerializeToString()
    )
    result = response_socket.recv_multipart()
    filename_received = result[0].decode('utf-8')
    file_data = result[1]
    print("Received %s" % filename_received)
    return file_data

# -----------------------------------------------
# POST
# -----------------------------------------------
""" -----------------------------------------------
POST
    file         [file] : string (e.g. "hello_world.txt")
    storage      [text] : string ("raid1" or "erasure_coding_rs")
    max_erasures [text] : int
    k            [text] : int
    delegate     [text] : bool
    send_to_node [text] : int (node to send 0..3)
 ----------------------------------------------- """
@app.route('/files', methods=['POST'])
def add_files():

    payload = request.form
    files = request.files
    
    # Make sure there is a file in the request
    if not files or not files.get('file'):
        logging.error("No file was uploaded in the request!")
        return make_response("File missing!", 400)
    
    file = files.get('file')
    filename = file.filename
    content_type = file.mimetype
    data = bytearray(file.read())
    size = len(data)
    print("File received: %s, size: %d bytes, type: %s" % (filename, size, content_type))
    
    # Read the requested storage mode from the form (default value: 'raid1')
    storage_mode = payload.get('storage', 'raid1')
    print("Storage mode: %s" % storage_mode)
  
    # Read amount of replicas, k
    k = int(payload.get('k', 1))
    print("Replicas, k: %d" % (k))

    # Read delegate, default : 0
    delegate = int(payload.get('delegate', '0'))

    # Read delegate, default : 0
    send_to_node = int(payload.get('send_to_node', '0'))

    if storage_mode == 'raid1':
        # -----------------------------------------------
        # TASK 1.1: GENERATE k REPLICAS
        # -----------------------------------------------
        if delegate!=1:
            print("DONT DELEGATE")

            task = messages_pb2.storedata_request()
            task.filename = random_string(8)
            task.is_delegate = False
            task.replications = k
            task.nodes_visited[:] = []
            task.storage_mode = storage_mode

            # Ensure requested replicas dosent exceed number of nodes
            assert(k<=STORAGE_NODES_NUM)

            # Store on nodes in random order
            visit = node_ids.copy()
            random.shuffle(visit)

            for i in range(k):
                resp = send_file_to_node(visit[i], data, task, del_socket, del_response_socket)
                print('Received: %s' % resp)

                storage_details = {
                    "filename": resp
                }

                cursor = insert_into_db(filename, size, content_type, storage_mode, storage_details)

        # -----------------------------------------------
        # TASK 1.2: DELEGATE
        # -----------------------------------------------
        else:
            print("DELEGATE")
            file_data_name = random_string(8)

            # Send a message to a storage node requesting it to store the data
            task = messages_pb2.storedata_request()
            task.filename = random_string(8)
            task.is_delegate = True
            task.replications = k
            task.nodes_visited[:] = []
            task.storage_mode = storage_mode

            header = messages_pb2.header()
            header.request_type = messages_pb2.STORE_FRAGMENT_DATA_REQ
            
            # SEND TO NODE
            del_socket.send_multipart([node_ids[send_to_node].encode('UTF-8'),
                header.SerializeToString(),
                task.SerializeToString(),
                data
            ])

            file_data_name = del_response_socket.recv_string()
            print('Received: %s' % resp)

            storage_details = {
                "filename": file_data_name
            }

    # -----------------------------------------------
    # TASK 2: USE ERASURE CODING
    # -----------------------------------------------
    elif storage_mode == 'erasure_coding_rs':

        # max_erasures = l
        # default: 1
        max_erasures = int(payload.get('max_erasures', 1))
        print("Max erasures: %d" % (max_erasures))


        fragment_names = reedsolomon.store_file(data, max_erasures, send_task_socket, response_socket)

        storage_details = {
            "coded_fragments": fragment_names,
            "max_erasures": max_erasures
        }

        cursor = insert_into_db(filename, size, content_type, storage_mode, storage_details)

    else:
        logging.error("Unexpected storage mode: %s" % storage_mode)
        return make_response("Wrong storage mode", 400)

    #cursor = insert_into_db(filename, size, content_type, storage_mode, storage_details)
    return make_response({"id": cursor.lastrowid }, 201)

# -----------------------------------------------
# GET
# -----------------------------------------------
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
        
        filename = storage_details['filename']
        print(filename)

        file_data = get_file(
            filename,
            f['storage_mode'], 
            data_req_socket, 
            response_socket
        )

    elif f['storage_mode'] == 'erasure_coding_rs':
        
        coded_fragments = storage_details['coded_fragments']
        max_erasures = storage_details['max_erasures']

        file_data = reedsolomon.get_file(
            coded_fragments,
            max_erasures,
            f['size'],
            data_req_socket, 
            response_socket
        )

    return send_file(io.BytesIO(file_data), mimetype=f['content_type'])
#

# -----------------------------------------------
# REPAIR USING REED-SOLOMON
# -----------------------------------------------
@app.route('/services/rs_repair',  methods=['GET'])
def rs_repair():
    #Retrieve the list of files stored using Reed-Solomon from the database
    db = get_db()
    cursor = db.execute("SELECT `id`, `storage_details`, `size` FROM `file` WHERE `storage_mode`='erasure_coding_rs'")
    if not cursor: 
        return make_response({"message": "Error connecting to the database"}, 500)
    
    rs_files = cursor.fetchall()
    rs_files = [dict(file) for file in rs_files]

    fragments_missing, fragments_repaired = reedsolomon.start_repair_process(rs_files,
                                                                             repair_socket,
                                                                             repair_response_socket)

    return make_response({"fragments_missing": fragments_missing,
                          "fragments_repaired": fragments_repaired})

@app.errorhandler(500)
def server_error(e):
    logging.exception("Internal error: %s", e)
    return make_response({"error": str(e)}, 500)


# Start the Flask app (must be after the endpoint functions)
host_local_computer = "localhost"  # Listen for connections on the local computer
host_local_network = "0.0.0.0"  # Listen for connections on the local network
app.run(
    host=host_local_network if is_raspberry_pi() else host_local_computer, port=9000
)

