"""
Aarhus University - Distributed Storage course - Lab 6

REST API + RAID Controller
"""

import argparse
import atexit  # unregister scheduler at app exit
import base64
import io  # For sending binary data in a HTTP response
import itertools as it
import logging
import math  # For cutting the file in half
import os
import random
import sqlite3
import string
import sys
import time  # For waiting a second for ZMQ connections
import uuid
from hashlib import sha256
from pprint import pp
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import zmq  # For ZMQ
# from apscheduler import Task
from apscheduler.schedulers.background import BackgroundScheduler
# from apscheduler.schedulers.sync import Scheduler
from apscheduler.triggers.interval import IntervalTrigger
# from apscheduler.schedulers.background import \
#     BackgroundScheduler  # automated repair
from flask import Flask, Response, g, make_response, request, send_file

import messages_pb2  # Generated Protobuf messages
# import rlnc
import constants
# from reedsolomon import ReedSolomonStorageProvider
from data_models.storage_id import StorageId
from raid1 import Raid1StorageProvider
from utils import create_logger, flatten_list, is_raspberry_pi, get_log_level_name_from_effective_level


def get_db(filename: str = "files.db") -> sqlite3.Connection:
    """Get a database connection. Create it if it doesn't exist."""
    if (
        "db" not in g
    ):  # g is a special object that is unique for each request, and can be used to store data.
        g.db = sqlite3.connect(filename, detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row

    return g.db


name_of_this_script: str = os.path.basename(__file__)
# remove .py extension if it exists
if name_of_this_script.endswith(".py"):
    name_of_this_script = name_of_this_script[:-3]


logger = create_logger(name_of_this_script)

logger.info("Initializing ZMQ sockets ...")
# Initiate ZMQ sockets
context = zmq.Context()

# Socket to receive setup message from storage nodes, when they come online.
# TODO: figure out how to register a callback
setup_addr_socket = context.socket(zmq.PULL)
setup_addr_socket.bind("tcp://*:5556")

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

sock_dealer_request_heartbeat = context.socket(zmq.DEALER)
sock_dealer_request_heartbeat.bind("tcp://*:5570")
# Set the timeout for the recv() method to 1 second
sock_dealer_request_heartbeat.RCVTIMEO = 1000

logger.info("Initializing ZMQ sockets ... DONE")
# Wait for all workers to start and connect.
time.sleep(1)
logger.info("Listening to ZMQ messages on tcp://*:5558 and tcp://*:5561")


parser = argparse.ArgumentParser(prog=name_of_this_script)


parser.add_argument(
    "-k",
    "--replication-factor",
    choices=[2, 3, 4],
    type=int,
    help="Number of fragments to store",
)
parser.add_argument(
    "-l",
    "--node-losses",
    type=int,
    choices=[1, 2],
    help="Number of fragments to recover",
)
parser.add_argument(
    "-m",
    "--mode",
    type=str,
    required=True,
    choices=constants.STORAGE_MODES,
    help="Mode of operation: raid1, erasure_coding_rs, erasure_coding_rlnc",
)


args = parser.parse_args()


logger.debug("Instantiating Flask app")
# Instantiate the Flask app (must be before the endpoint functions)
app = Flask(__name__)
# Close the DB connection after serving the request
# app.teardown_appcontext(close_db)


@app.teardown_appcontext
def close_db(e=None) -> None:
    db = g.pop("db", None)

    if db is not None:
        db.close()


@app.route("/files", methods=["GET"])
def list_files() -> Response:
    """
    Get metadata for all files in storage system.
    """
    db = get_db()
    cursor = db.execute("SELECT * FROM `file`")
    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    files = cursor.fetchall()
    # Convert files from sqlite3.Row object (which is not JSON-encodable) to
    # a standard Python dictionary simply by casting
    files = [dict(file) for file in files]

    return make_response({"files": files})


@app.route("/files/<int:file_id>", methods=["GET"])
def download_file(file_id: int) -> Response:
    logger.info(
        f"Received request to download file {file_id}. Storage mode is {args.mode}"
    )

    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
    if not cursor:
        error_msg: str = "Error connecting to the database"
        logger.error(error_msg)
        return make_response({"message": error_msg}, 500)

    database_row = cursor.fetchone()
    if not database_row:
        error_msg: str = "File {} not found".format(file_id)
        logger.error(error_msg)
        return make_response({"message": error_msg}, 404)

    database_row = dict(database_row)

    storage_details = database_row["storage_details"]

    match args.mode:
        case "raid1":
            storage_ids: List[StorageId] = [
                StorageId.from_string(id) for id in storage_details.split(";")
            ]
            replication_factor: float = math.sqrt(len(storage_ids))
            assert replication_factor.is_integer() and replication_factor in [
                2,
                3,
                4,
            ], "Invalid number of fragments"
            replication_factor = int(replication_factor)

            # reshape the list of filenames into a 2D array of size (replication_factor, replication_factor)
            list_of_storage_ids: List[List[StorageId]] = [
                storage_ids[i : i + replication_factor]
                for i in range(0, len(storage_ids), replication_factor)
            ]

            provider = Raid1StorageProvider(
                replication_factor, send_task_socket, response_socket, data_req_socket
            )
            file_data = provider.get_file(list_of_storage_ids)

        case "fake-hdfs":

            pass
        case "erasure_coding_rs":
            pass
        case "erasure_coding_rlnc":
            pass
        case _:
            error_msg: str = "Invalid storage mode"
            logger.error(error_msg)
            return make_response({"message": error_msg}, 500)

    # This is the hash of the file that is reconstructed
    file_hash: str = sha256(file_data).hexdigest()
    assert file_hash == database_row["hash"], "File hash mismatch"

    logger.info(f"File {file_id} downloaded successfully")
    return send_file(io.BytesIO(file_data), mimetype=database_row["content_type"])


# if f['storage_mode'] == 'raid1':

#     part1_filenames = storage_details['part1_filenames']
#     part2_filenames = storage_details['part2_filenames']

#     file_data = raid1.get_file(
#         part1_filenames,
#         part2_filenames,
#         data_req_socket,
#         response_socket
#     )

# elif f['storage_mode'] == 'erasure_coding_rs':

#     coded_fragments = storage_details['coded_fragments']
#     max_erasures = storage_details['max_erasures']

#     file_data = reedsolomon.get_file(
#         coded_fragments,
#         max_erasures,
#         f['size'],
#         data_req_socket,
#         response_socket
#     )

# elif f['storage_mode'] == 'erasure_coding_rlnc':

#     coded_fragments = storage_details['coded_fragments']
#     max_erasures = storage_details['max_erasures']

#     file_data = rlnc.get_file(
#         coded_fragments,
#         max_erasures,
#         f['size'],
#         data_req_socket,
#         response_socket
#     )

# return send_file(io.BytesIO(file_data), mimetype=f['content_type'])
#

# HTTP HEAD requests are served by the GET endpoint of the same URL,
# so we'll introduce a new endpoint URL for requesting file metadata.


# @app.route("/files/<int:file_id>/info", methods=["GET"])
# def get_file_metadata(file_id: int):

#     db = get_db()
#     cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
#     if not cursor:
#         return make_response({"message": "Error connecting to the database"}, 500)

#     f = cursor.fetchone()
#     if not f:
#         return make_response({"message": "File {} not found".format(file_id)}, 404)

#     # Convert to a Python dictionary
#     f = dict(f)
#     print("File: %s" % f)

#     return make_response(f)


# @app.route("/files_mp", methods=["POST"])
# def add_files_multipart():
#     # Flask separates files from the other form fields
#     payload = request.form
#     files = request.files

#     # Make sure there is a file in the request
#     if not files or not files.get("file"):
#         logging.error("No file was uploaded in the request!")
#         return make_response("File missing!", 400)

#     # Reference to the file under 'file' key
#     file = files.get("file")
#     # The sender encodes a the file name and type together with the file contents
#     filename = file.filename
#     content_type = file.mimetype
#     # Load the file contents into a bytearray and measure its size
#     data = bytearray(file.read())
#     size = len(data)
#     print(
#         "File received: %s, size: %d bytes, type: %s" % (filename, size, content_type)
#     )

#     # Read the requested storage mode from the form (default value: 'raid1')
#     storage_mode = payload.get("storage", "raid1")
#     print("Storage mode: %s" % storage_mode)

#     if storage_mode == "raid1":
#         file_data_1_names, file_data_2_names = raid1.store_file(
#             data, send_task_socket, response_socket
#         )

#         storage_details = {
#             "part1_filenames": file_data_1_names,
#             "part2_filenames": file_data_2_names,
#         }

#     elif storage_mode == "erasure_coding_rs":
#         # Reed Solomon code
#         # Parse max_erasures (everything is a string in request.form,
#         # we need to convert to int manually), set default value to 1
#         max_erasures = int(payload.get("max_erasures", 1))
#         print("Max erasures: %d" % (max_erasures))

#         # Store the file contents with Reed Solomon erasure coding
#         fragment_names = reedsolomon.store_file(
#             data, max_erasures, send_task_socket, response_socket
#         )

#         storage_details = {
#             "coded_fragments": fragment_names,
#             "max_erasures": max_erasures,
#         }

#     elif storage_mode == "erasure_coding_rlnc":
#         # RLNC
#         max_erasures = int(payload.get("max_erasures", 1))
#         print("Max erasures: %d" % (max_erasures))

#         subfragments_per_node = int(payload.get("subfragments_per_node", 3))
#         print("Subfragments per node: %d" % (subfragments_per_node))

#         # Store the file contents with Random Linear Network Coding encoding
#         fragment_names = rlnc.store_file(
#             data, max_erasures, subfragments_per_node, send_task_socket, response_socket
#         )

#         storage_details = {
#             "coded_fragments": fragment_names,
#             "max_erasures": max_erasures,
#             "subfragments_per_node": subfragments_per_node,
#         }

#         print(f"File stored: {storage_details}")

#     else:
#         logging.error("Unexpected storage mode: %s" % storage_mode)
#         return make_response("Wrong storage mode", 400)

#     # Insert the File record in the DB
#     import json

#     db = get_db()
#     cursor = db.execute(
#         "INSERT INTO `file`(`filename`, `size`, `content_type`, `storage_mode`, `storage_details`) VALUES (?,?,?,?,?)",
#         (filename, size, content_type, storage_mode, json.dumps(storage_details)),
#     )
#     db.commit()

#     return make_response({"id": cursor.lastrowid}, 201)


@app.route("/files", methods=["POST"])
def add_files() -> Response:
    """
    Add a new file to the storage system.
    """
    payload: Any | None = request.get_json()
    filename: str = payload.get("filename")
    content_type: str = payload.get("content_type")
    file_data: bytes = base64.b64decode(payload.get("contents_b64"))
    filesize: int = len(file_data)
    uid: uuid.UUID = uuid.uuid4()
    logger.info(
        f"File received: {filename}, size: {filesize} bytes, type: {content_type}"
    )

    match args.mode:
        case "raid1":
            provider = Raid1StorageProvider(
                args.replication_factor,
                send_task_socket,
                response_socket,
                data_req_socket,
            )
            list_of_storage_ids = provider.store_file(file_data, uid=uid)

            storage_ids: List[StorageId] = []

            for i, stripe_names in enumerate(list_of_storage_ids):
                for j, _ in enumerate(stripe_names):
                    storage_id = StorageId(uid, i, j)
                    storage_ids.append(storage_id)

            logger.debug(f"Storage details: {storage_ids}")
        case "fake-hdfs":
            pass

        case "erasure_coding_rs":
            pass
        case "erasure_coding_rlnc":
            pass
        case _:
            logging.error(f"Unexpected storage mode: {args.mode}")
            return make_response("Wrong storage mode", 400)

    file_hash: str = sha256(file_data).hexdigest()
    logger.debug(f"File hash: {file_hash}")
    storage_details: str = ";".join([str(storage_id) for storage_id in storage_ids])
    logger.debug(f"Storage details: {storage_details}")

    db = get_db()
    cursor = db.execute(
        "INSERT INTO `file`(`filename`, `size`, `content_type`, `storage_mode`, `storage_details`, `hash`) VALUES (?,?,?,?,?,?)",
        (filename, filesize, content_type, args.mode, storage_details, file_hash),
    )

    db.commit()

    logger.info(
        f"File stored: {filename}, size: {filesize} bytes, type: {content_type}, with id: {cursor.lastrowid}"
    )

    # Return the ID of the new file record with HTTP 201 (Created) status code
    return make_response({"id": cursor.lastrowid}, 201)


# @app.route("/services/rlnc_repair", methods=["GET"])
# def rlnc_repair():
#     # Retrieve the list of files stored using RLNC from the database
#     db = get_db()
#     cursor = db.execute(
#         "SELECT `id`, `storage_details`, `size` FROM `file` WHERE `storage_mode`='erasure_coding_rlnc'"
#     )
#     if not cursor:
#         return make_response({"message": "Error connecting to the database"}, 500)

#     rlnc_files = cursor.fetchall()
#     rlnc_files = [dict(file) for file in rlnc_files]

#     fragments_missing, fragments_repaired = rlnc.start_repair_process(
#         rlnc_files, repair_socket, repair_response_socket
#     )

#     return make_response(
#         {
#             "fragments_missing": fragments_missing,
#             "fragments_repaired": fragments_repaired,
#         }
#     )


#


# @app.route("/services/rs_repair", methods=["GET"])
# def rs_repair():
#     # Retrieve the list of files stored using Reed-Solomon from the database
#     db = get_db()
#     cursor = db.execute(
#         "SELECT `id`, `storage_details`, `size` FROM `file` WHERE `storage_mode`='erasure_coding_rs'"
#     )
#     if not cursor:
#         return make_response({"message": "Error connecting to the database"}, 500)

#     rs_files = cursor.fetchall()
#     rs_files = [dict(file) for file in rs_files]

#     fragments_missing, fragments_repaired = reedsolomon.start_repair_process(
#         rs_files, repair_socket, repair_response_socket
#     )

#     return make_response(
#         {
#             "fragments_missing": fragments_missing,
#             "fragments_repaired": fragments_repaired,
#         }
#     )


#


# def rs_automated_repair():
#     print("Running automated Reed-Solomon repair process")
#     with app.app_context():
#         rs_repair()


#


# Create a scheduler and post a repair job every 60 seconds
# scheduler = BackgroundScheduler()
# scheduler.add_job(func=rs_automated_repair, trigger="interval", seconds=60)
# Temporarily disabled scheduler
# scheduler.start()

# Shut down the scheduler when exiting the app
# atexit.register(lambda: scheduler.shutdown())


@app.errorhandler(500)
def server_error(e) -> Response:
    logger.exception("Internal error: %s", e)
    return make_response({"error": str(e)}, 500)


def task_send_heartbeat_request() -> None:
    logger.info("Sending heartbeat request to storage nodes ...")
    # sock_dealer_request_heartbeat.send_string("heartbeat")

    storage_nodes_online: Set[uuid.UUID] = {}
    return

    # # We have 4 nodes in total
    num_timeout_reached = 0
    for i in range(constants.TOTAL_NUMBER_OF_STORAGE_NODES):
        # pass
        try:
            reply = sock_dealer_request_heartbeat.recv()
        except zmq.ZmqError:
            # If the timeout of 1000 ms is reached we deem the node as being offline
            num_timeout_reached += 1
            logger.warn(f"Timeout reached [{num_timeout_reached}]")
        else:
            # Expect reply to be of type HeartBeatResponse
            resp = messages_pb2.HeartBeatResponse()
            resp.ParseFromString(reply)
            uid = uuid.UUID(bytes=resp.uid)
            storage_nodes_online = storage_nodes_online & uid

    # Figure out which storage nodes have not replied
    logger.info("Sending heartbeat request to storage nodes ... DONE")


scheduler = BackgroundScheduler()

interval: int = 10  # seconds

scheduler.add_job(
    func=task_send_heartbeat_request,
    trigger=IntervalTrigger(seconds=interval),
    id="heartbeat-request",
    name=f"A job that runs every {interval} seconds",
    replace_existing=True,
)

app.before_first_request(scheduler.start)
app.before_first_request(lambda: logger.error("TODO: figure out which storage nodes are online."))

# @app.before_first_request
# def activate_scheduler():
#     scheduler.start()


# @app.teardown_appcontext
# def shutdown_scheduler(exception=None):
#     logger.info("Shutting down background job scheduler")
#     scheduler.shutdown()


atexit.register(scheduler.shutdown)

# with Scheduler() as scheduler:
#     # Add schedules, configure tasks here
#     scheduler.start_in_background()
#     task = Task()
#     trigger = IntervalTrigger(seconds=60)


# Start the Flask app (must be after the endpoint functions)
host_local_computer = "localhost"  # Listen for connections on the local computer
host_local_network = "0.0.0.0"  # Listen for connections on the local network
app.run(
    host=host_local_network if is_raspberry_pi() else host_local_computer, port=9000
)
