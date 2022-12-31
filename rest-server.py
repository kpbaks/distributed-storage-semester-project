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

import reedsolomon

# from storage_providers.reedsolomon  import ReedSolomonStorageProvider

import zmq  # For ZMQ
# from apscheduler import Task
from apscheduler.schedulers.background import BackgroundScheduler
# from apscheduler.schedulers.sync import Scheduler
from apscheduler.triggers.interval import IntervalTrigger
# from apscheduler.schedulers.background import \
#     BackgroundScheduler  # automated repair
from flask import (Flask, Request, Response, g, make_response, redirect,
                   request, send_file)

# import rlnc
import constants
import messages_pb2  # Generated Protobuf messages
# from reedsolomon import ReedSolomonStorageProvider
from data_models.storage_id import StorageId
from data_models.storage_node import StorageNode
from raid1 import Raid1StorageProvider
from utils import (create_logger, flatten_list,
                   get_log_level_name_from_effective_level, is_raspberry_pi)



NAME_OF_THIS_SCRIPT: str = os.path.basename(__file__).split(".")[0]

parser = argparse.ArgumentParser(prog=NAME_OF_THIS_SCRIPT)

parser.add_argument(
    "-k",
    "--replication-factor",
    choices=[2, 3, 4],
    type=int,
    help="Number of fragments to store",
)
parser.add_argument(
    "-l",
    "--max-erasures",
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
    help=f"Mode of operation: {constants.STORAGE_MODES}",
)

args = parser.parse_args()

logger = create_logger(NAME_OF_THIS_SCRIPT)

# ------------------ ZMQ --------------------------------

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


# ------------------ Flask --------------------------------
logger.debug("Instantiating Flask app")
# Instantiate the Flask app (must be before the endpoint functions)
app = Flask(__name__)
# Close the DB connection after serving the request
# app.teardown_appcontext(close_db)


def get_db(filename: str = constants.SQLITE_DB_PATH) -> sqlite3.Connection:
    """Get a database connection. Create it if it doesn't exist."""
    if (
        "db" not in g
    ):  # g is a special object that is unique for each request, and can be used to store data.
        g.db = sqlite3.connect(filename, detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row

    return g.db


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
    cursor = db.execute("SELECT * FROM `file_metadata`")
    if not cursor:
        err_msg: str = "Error connecting to the database"
        logger.error(err_msg)
        return make_response({"message": err_msg}, 500)

    files = cursor.fetchall()
    # Convert files from sqlite3.Row object (which is not JSON-encodable) to
    # a standard Python dictionary simply by casting
    files = [dict(file) for file in files]

    return make_response({"files": files})


def get_file_metadata_from_db(file_metadata_id: int) -> Optional[dict]:
    """
    Get file metadata from the database.
    """
    assert isinstance(file_metadata_id, int), f"file_metadata_id must be an int, not {type(file_metadata_id)}"
    db = get_db()
    cursor = db.execute(
        "SELECT * FROM `file_metadata` WHERE `file_metadata_id`=?", [file_metadata_id]
    )

    if not cursor:
        return None

    file_metadata = cursor.fetchone()
    if not file_metadata:
        return None

    # Convert file_metadata from sqlite3.Row object (which is not JSON-encodable) to
    # a standard Python dictionary simply by casting
    file_metadata = dict(file_metadata)

    return file_metadata


@app.route("/files/<int:file_id>/task1.1", methods=["GET"])
def download_file_task1_1(file_id: int) -> Response:
    """
    Download a file from the storage system using Task 1.1.
    """
    logger.info(f"Received request to download file {file_id} using Task 1.1")


    file_metadata = get_file_metadata_from_db(file_id)
    if not file_metadata:
        err_msg = f"File {file_id} not found"
        logger.error(err_msg)
        return make_response({"message": err_msg}, 404)

    db = get_db()

    # Get the storage nodes that have the file
    cursor = db.execute("""
        SELECT *
        FROM `storage_nodes` AS sn
        JOIN `replicas` AS r
            ON r.file_metadata_id = ?
        WHERE sn.storage_node_id = r.storage_node_id
        """,
        [file_id]
    )


    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    storage_nodes = cursor.fetchall()
    # Convert storage_nodes from sqlite3.Row object (which is not JSON-encodable) to
    # a standard Python dictionary simply by casting
    storage_nodes = [dict(sn) for sn in storage_nodes]

    pp(storage_nodes)

    # Permute the storage nodes so that the order is random
    random.shuffle(storage_nodes)

    # Create a client socket to send the request to the Storage Nodes
    for storage_node in storage_nodes:
        sock = context.socket(zmq.REQ)
        endpoint = f"tcp://{storage_node['address']}:{storage_node['port_get_data']}"
        logger.info(f"Connecting to Storage Node {storage_node['friendly_name']} at {endpoint}")
        sock.connect(endpoint)
        request = messages_pb2.Message(
            type=messages_pb2.MsgType.GET_DATA_REQUEST,
            get_data_request=messages_pb2.GetDataRequest(
                file_uid=file_metadata["uid"]
            )
        )
        # request = messages_pb2.GetDataRequest()
        # request.file_uid = file_metadata["uid"]
        request_serialized = request.SerializeToString()
        sock.send_multipart([request_serialized])

        # Set timeout for receiving the response from the Storage Node
        # sock.RCVTIMEO = time_to_wait(file_metadata["size"])
        response: List[bytes] = sock.recv_multipart()
        msg = messages_pb2.Message.FromString(response[0])
        if msg.type != messages_pb2.MsgType.GET_DATA_RESPONSE:
            logger.error(f"Received invalid response from Storage Node {storage_node['uid']}")
            continue
        
        
        if msg.get_data_response.success:
            file_data: bytes = msg.get_data_response.file_data
            content_type: str = file_metadata["content_type"]
            return send_file(io.BytesIO(file_data), mimetype=content_type)

        else:
            logger.error(f"Error receiving replica {file_metadata['uid']} from Storage Node {storage_node['uid']}")
        
        sock.close()

    return make_response({"message": "Error downloading file"}, 500)



@app.route("/files/<int:file_id>/task2.1", methods=["GET"])
def download_file_task2_1(file_id: int) -> Response:
    """
    Download a file from the storage system using Task 2.1.
    """
    logger.info(f"Received request to download file {file_id} using Task 2.1")

    # Get the file metadata from the database
    db = get_db()
    cursor = db.execute(""""
        SELECT * FROM `file_metadata`
        WHERE `file_id` = ?
    """,
     (file_id)
    )
    if not cursor:
        error_msg: str = "Error connecting to the database"
        logger.error(error_msg)
        return make_response({"message": error_msg}, 500)
        
    file_metadata = cursor.fetchone()
    if not file_metadata:
        error_msg: str = f"File {file_id} not found"
        logger.error(error_msg)
        return make_response({"message": error_msg}, 404)
    
    # Convert file_metadata from sqlite3.Row object (which is not JSON-encodable) to a standard Python dictionary simply by casting
    file_metadata = dict(file_metadata)
    filesize = file_metadata["filesize"]

    l: int = args.max_erasures # Should probably be stored in the database



    

    filedata_return = reedsolomon.get_file(fragment_names, l, filesize, data_req_socket, response_socket)


    return make_response({"message": "Not implemented"}, 501)


@app.route("/files/<int:file_id>/task2.2", methods=["GET"])
def download_file_task2_2(file_id: int) -> Response:
    """
    Download a file from the storage system using Task 2.2.
    """
    logger.info(f"Received request to download file {file_id} using Task 2.2")

    return make_response({"message": "Not implemented"}, 501)



@app.route("/files/<int:file_id>", methods=["GET"])
def download_file(file_id: int) -> Response:
    logger.info(
        f"Received request to download file {file_id}. Storage mode is {args.mode}"
    )

    match args.mode:
        case "task1.1":
            return redirect(f"/files/{file_id}/task1.1", code=307)
        case "task1.2":
            # The logic for Task 1.2 is the same as Task 1.1
            return redirect(f"/files/{file_id}/task1.1", code=307)
        case "task2.1":
            return redirect(f"/files/{file_id}/task2.1", code=307)
        case "task2.2":
            return redirect(f"/files/{file_id}/task2.2", code=307)
        case _:
            return make_response({"message": "Invalid mode"}, 400)


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
                storage_ids[i: i + replication_factor]
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


def get_storage_nodes_from_db() -> list[StorageNode]:
    """
    Returns a list of storage nodes from the database.
    """

    db = get_db()
    cursor = db.execute("SELECT * FROM `storage_nodes`")
    if not cursor:
        return []

    return [StorageNode(**dict(row)) for row in cursor.fetchall()]


def extract_fields_from_post_request(request: Request) -> tuple[str, str, bytes, int]:
    """
    Extracts the filename, content type and file data from the request.
    """
    
    # TODO: make it able to handle json
    payload = request.form

    # Check if request is a form or a json
    if not payload:
        payload = request.get_json()
        if not payload:
            logging.error("No file was uploaded in the request!")
            raise Exception("No file was uploaded in the request!")

    #payload: Any | None = request.get_json()
    filename: str = payload.get("filename")
    content_type: str = payload.get("content_type")
    file_data: bytes = base64.b64decode(payload.get("contents_b64"))
    size = len(file_data)  # The amount of bytes in the file

    return filename, content_type, file_data, size


def time_to_wait(filesize: int) -> int:
    """
    Calculates the time to wait for a file to be stored in one node,
    assuming that the network can take 10 mbps, and the filesize is in bytes.
    """

    return (filesize * 8) // 10**7


@app.route("/files_task1.1", methods=["POST"])
def add_files_task1_1() -> Response:

    filename, content_type, file_data, filesize = extract_fields_from_post_request(
        request
    )

    k: int = args.replication_factor

    storage_nodes: List[StorageNode] = get_storage_nodes_from_db()
    # Create list of replica # of IP addresses
    chosen_storage_nodes = random.sample(storage_nodes, k)

    file_uid = uuid.uuid4()

    def send_file_to_node(node: StorageNode) -> zmq.Socket:
        msg = messages_pb2.Message(
            type=messages_pb2.MsgType.STORE_DATA_REQUEST,
            store_data_request=messages_pb2.StoreDataRequest(
                file_uid=str(file_uid),
                file_data=file_data
            )
        )
        msg_serialized = msg.SerializeToString()

        # request = messages_pb2.StoreDataRequest()
        # request.file_uid = str(file_uid)
        # request.file_data = file_data

        # serialized_request = request.SerializeToString()

        sock = context.socket(zmq.REQ)
        sock.connect(f"tcp://{node.address}:{node.port_store_data}")
        sock.send_multipart([msg_serialized])

        return sock

    
    # Send file to chosen_storage_nodes
    for node in chosen_storage_nodes:
        client: zmq.Socket = send_file_to_node(node)
        logger.debug(f"Sent file to {node.address}:{node.port_store_data}")
        resp = client.recv_multipart() # await response
        resp_serialized = messages_pb2.Message.FromString(resp[0])
        logger.debug(f"Received response: {resp_serialized}")

    # for _ in range(k):
    #     resp = response_socket.recv_string()
    #     logger.info(f"Received: {resp}")    
    
    db = get_db()
    cursor = db.execute(
        """
        INSERT INTO
            `file_metadata`(
                `filename`,
                `size`,
                `content_type`,
                `uid`,
                `storage_mode`
            )
        VALUES
            (?,?,?,?,?)
        """,
        (filename, filesize, content_type, str(file_uid), "replication"),
    )

    db.commit()

    file_id: int = cursor.lastrowid
    
    for node in chosen_storage_nodes:
        pp(node)
        logger.debug(f"File ID: {file_id}, Node ID: {node.uid}")

        db.execute(
            f"""
        INSERT INTO
            `replicas` (
                `file_metadata_id`,
                'storage_node_id'
            )
        VALUES (
            (SELECT file_metadata_id FROM file_metadata WHERE file_metadata_id = ?),
            (SELECT storage_node_id FROM storage_nodes WHERE uid = ?)
        )
        """,
            (file_id, node.uid),
        )

        db.commit()

    # db.commit()

    return make_response({"id": file_id}, 201)


@app.route("/files_task1.2", methods=["POST"])
def add_files_task1_2() -> Response:
    """
    Add a new file to the storage system.
    """
    filename, content_type, file_data, filesize = extract_fields_from_post_request(
        request
    )

    k: int = args.replication_factor

    storage_nodes: List[StorageNode] = get_storage_nodes_from_db()
    assert len(storage_nodes) >= k, "Not enough storage nodes to store the file!"
    random.shuffle(storage_nodes)

    first_k_nodes = storage_nodes[:k]
    first_node = first_k_nodes[0]
    rest_nodes = first_k_nodes[1:]

    file_uid = uuid.uuid4()

    msg = messages_pb2.Message(
        type=messages_pb2.MsgType.DELEGATE_STORE_DATA_REQUEST,
        payload=messages_pb2.DelegateStoreDataRequest(
            file_uid=str(file_uid).encode("UTF-8"),
            file_data=file_data,
            nodes_to_forward_to=[
                messages_pb2.StorageNode(
                    uid=node.uid.encode("UTF-8"),
                    ipv4=node.ipv4.encode("UTF-8"),
                    port=node.port,
                ) for node in rest_nodes
            ],
        )
    )

    msg_serialized = msg.SerializeToString()

    # delegate_store_data_request = messages_pb2.DelegateStoreDataRequest()
    # delegate_store_data_request.file_uid = str(file_uid).encode("UTF-8")
    # delegate_store_data_request.file_data = file_data

    # delegate_store_data_request.nodes_to_forward_to = [
    #     messages_pb2.StorageNode(
    #         uid=node.uid.encode("UTF-8"),
    #         ipv4 = node.ipv4.encode("UTF-8"),
    #         port=node.port,
    #     ) for node in rest_nodes
    # ]

    # serialized_request = delegate_store_data_request.SerializeToString()

    # Create a REQ socket to send the request to the first node
    sock = context.socket(zmq.REQ)
    sock.connect(f"tcp://{first_node.ipv4}:{first_node.port}")
    sock.send_multipart([msg_serialized])

    # Wait for the response
    received = sock.recv_multipart()
    try:
        response = messages_pb2.Message.FromString(received[0])
        # response = messages_pb2.DelegateStoreDataResponse.FromString(received[0])
    except messages_pb2.DecodeError as e:
        logger.error(f"Error decoding response: {e}")
        return make_response({"error": "Error decoding response"}, 500)
    else:
        if response.type != messages_pb2.MsgType.DELEGATE_STORE_DATA_RESPONSE:
            logger.error(f"Unexpected response type: {response.type}")
            return make_response({"error": "Unexpected response type"}, 500)
        
        logger.info(f"Received response: {response}")
        db = get_db()
        cursor = db.execute("""
        INSERT INTO
            `file_metadata`(
                `filename`,
                `size`,
                `content_type`,
                `uid`,
                `storage_mode`
            )
        VALUES
            (?,?,?,?,?)
        """,
            [filename, filesize, content_type, str(file_uid), "replication_with_delegation"]
        )

        db.commit()

        file_id: int = cursor.lastrowid

        for node in storage_nodes:
            db.execute(
                f"""
            INSERT INTO
                `replicas` (
                    `file_metadata_id`,
                    'storage_node_id'
                )
            VALUES (
                (SELECT file_metadata_id FROM file_metadata WHERE file_metadata_id = ?),
                (SELECT storage_node_id FROM storage_nodes WHERE uid = ?)
            )
            """,
                (file_id, node.uid),
            )

        db.commit()
        logger.info(f"Inserted file with id {file_id} into database")
        return make_response({"id": cursor.lastrowid}, 201)
    finally:
        sock.close()



@app.route("/files_task2.1", methods=["POST"])
def add_files_task2_1() -> Response:
    """
    Add a new file to the storage system.
    """
    filename, content_type, file_data, filesize = extract_fields_from_post_request(
        request
    )

    l: int = args.max_erasures
    data = bytearray(file_data)

    fragment_names, fragment_data = reedsolomon.store_file(data, l, send_task_socket, response_socket)
    
    logger.info(f"fragment_data: {fragment_data}")

    def send_file_to_node(node: StorageNode, fragment_name: str, fragment_data: bytes) -> None:
        request = messages_pb2.StoreDataRequest()
        logger.debug(f"Sending file to node {node.uid} with fragment name {fragment_name}")

        request.file_uuid = str(fragment_name)
        request.file_data = fragment_data

        serialized_request = request.SerializeToString()

        send_task_socket.send_multipart(
            [str(node.uid).encode("UTF-8"), serialized_request]
        )

    storage_nodes = get_storage_nodes_from_db()

    for node, fragment_name, fragment in zip(storage_nodes, fragment_names, fragment_data):
        send_file_to_node(node, fragment_name, fragment)

    for _ in range(constants.TOTAL_NUMBER_OF_STORAGE_NODES):
        resp = response_socket.recv_string()
        print("Received: %s" % resp)

    db = get_db()
    cursor = db.execute(
        """
        INSERT INTO
            `file_metadata`(
                `filename`,
                `size`,
                `content_type`,
                `storage_mode`
            )
        VALUES
            (?,?,?,?)
        """,
        (filename, filesize, content_type, "reedsolomon"),
    )

    db.commit()

    file_id: int = cursor.lastrowid

    for node in storage_nodes:
        db.execute(
            f"""
        INSERT INTO
            `replicas` (
                `file_metadata_id`,
                'storage_node_id'
            )
        VALUES (
            (SELECT file_metadata_id FROM file_metadata WHERE file_metadata_id = ?),
            (SELECT storage_node_id FROM storage_nodes WHERE uid = ?)
        )
        """,
            (file_id, node.uid),
        )

    db.commit()

    filedata_return = reedsolomon.get_file(fragment_names, l, filesize, data_req_socket, response_socket)
    logger.info(f"filedata_return: {filedata_return}")

    return make_response({"id": file_id}, 201)
    

@app.route("/files_task2.2", methods=["POST"])
def add_files_task2_2() -> Response:

    filename, content_type, file_data, filesize = extract_fields_from_post_request(
        request
    )

    return make_response({"id": cursor.lastrowid}, 201)


@app.route("/files", methods=["POST"])
def add_files() -> Response:
    """
    Add a new file to the storage system.
    """

    logger.info(f"mode: {args.mode}")
    match args.mode:
        case "task1.1":
            return redirect("/files_task1.1", code=307)
        case "task1.2":
            return redirect("/files_task1.2", code=307)
        case "task2.1":
            return redirect("/files_task2.1", code=307)
        case "task2.2":
            return redirect("/files_task2.2", code=307)
        case _:
            return make_response("Wrong storage mode", 400)

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
    storage_details: str = ";".join(
        [str(storage_id) for storage_id in storage_ids])
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


@app.route('/services/rs_repair',  methods=['GET'])
def rs_repair() -> Response:
    #Retrieve the list of files stored using Reed-Solomon from the database
    db = get_db()
    cursor = db.execute("SELECT `id`, `storage_details`, `size` FROM `file` WHERE `storage_mode`='reedsolomon'")
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

atexit.register(scheduler.shutdown)

# app.before_first_request(scheduler.start)
# app.before_first_request(
#     lambda: logger.error("TODO: figure out which storage nodes are online.")
# )

sock_pull_storage_node_advertisement = context.socket(zmq.REP)
sock_pull_storage_node_advertisement.bind(f"tcp://*:{constants.PORT_STORAGE_NODE_ADVERTISEMENT}")

for i in range(constants.TOTAL_NUMBER_OF_STORAGE_NODES):
    logger.debug(f"Waiting for storage node advertisement ... [{i+1}/{constants.TOTAL_NUMBER_OF_STORAGE_NODES}]")

    # Receive the advertisement from the storage node
    msg = sock_pull_storage_node_advertisement.recv() # TODO: add timeout
    # Expect msg to be of type StorageNodeAdvertisement
    advertisement = messages_pb2.StorageNodeAdvertisementRequest()
    advertisement.ParseFromString(msg)
    uid = uuid.UUID(advertisement.node.uid.replace("\n", ""))
    port_get_data: int = advertisement.node.port_get_data
    port_store_data: int = advertisement.node.port_store_data

    ipv4_addr: str = advertisement.node.ipv4
    friendly_name: str = advertisement.friendly_name

    # Check if the storage node is already in the database
    db = sqlite3.connect("files.db", detect_types=sqlite3.PARSE_DECLTYPES)
    db.row_factory = sqlite3.Row

    print(f"{str(uid)} and len ({len(str(uid))})")


    cursor = db.execute("""
        SELECT * FROM `storage_nodes` WHERE `uid`= ? AND `address`=? AND `port_get_data`=?
    """, [str(uid), ipv4_addr, port_get_data]
    )

    # If the storage node is not in the database, add it
    if not cursor.fetchone():
        db.execute("""
        INSERT INTO `storage_nodes`(`uid`, `friendly_name`, `address`, `port_get_data`, `port_store_data`) VALUES (?,?,?,?,?)
        """, (str(uid), friendly_name, ipv4_addr, port_get_data, port_store_data)
        )
        db.commit()

    # Send the response to the storage node
    resp = messages_pb2.StorageNodeAdvertisementResponse()
    resp.success = True
    sock_pull_storage_node_advertisement.send(resp.SerializeToString())

    logger.info(f"Storage node {friendly_name} is online")


def drop_storage_nodes_table() -> None:
    db = sqlite3.connect("files.db", detect_types=sqlite3.PARSE_DECLTYPES)
    db.row_factory = sqlite3.Row

    db.execute("DROP TABLE IF EXISTS `storage_nodes`")
    db.commit()
    logger.info("Dropped table `storage_nodes`")

atexit.register(drop_storage_nodes_table)

# Start the Flask app (must be after the endpoint functions)
host_local_computer = "localhost"  # Listen for connections on the local computer
host_local_network = "0.0.0.0"  # Listen for connections on the local network
app.run(
    host=host_local_network if is_raspberry_pi() else host_local_computer, port=9000
)
