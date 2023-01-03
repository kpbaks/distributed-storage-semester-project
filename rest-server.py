import argparse
import atexit  # unregister scheduler at app exit
import base64
import io  # For sending binary data in a HTTP response
import logging
import os
import random
import sqlite3
import time  # For waiting a second for ZMQ connections
import uuid
from typing import List, Optional, Tuple

import zmq  # For ZMQ
# from apscheduler import Task
# from apscheduler.schedulers.background import BackgroundScheduler
# # from apscheduler.schedulers.sync import Scheduler
# from apscheduler.triggers.interval import IntervalTrigger
# from apscheduler.schedulers.background import \
#     BackgroundScheduler  # automated repair
from flask import (Flask, Request, Response, g, make_response, redirect,
                   request, send_file)

# import rlnc
import constants
import messages_pb2 as protobuf_msgs  # Generated Protobuf messages
import reedsolomon
# from reedsolomon import ReedSolomonStorageProvider
from data_models.storage_id import StorageId
from data_models.storage_node import StorageNode
# from raid1 import Raid1StorageProvider
from utils import (create_logger, flatten_list,
                   get_log_level_name_from_effective_level, is_raspberry_pi)

# from storage_providers.reedsolomon  import ReedSolomonStorageProvider


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
    assert isinstance(
        file_metadata_id, int
    ), f"file_metadata_id must be an int, not {type(file_metadata_id)}"
    db = get_db()
    cursor = db.execute(
        "SELECT * FROM `file_metadata` WHERE `file_metadata_id`=?", [
            file_metadata_id]
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
    cursor = db.execute(
        """
        SELECT *
        FROM `storage_nodes` AS sn
        JOIN `replicas` AS r
            ON r.file_metadata_id = ?
        WHERE sn.storage_node_id = r.storage_node_id
        """,
        [file_id],
    )

    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    storage_nodes = cursor.fetchall()
    # Convert storage_nodes from sqlite3.Row object (which is not JSON-encodable) to
    # a standard Python dictionary simply by casting
    storage_nodes = [dict(sn) for sn in storage_nodes]

    # Permute the storage nodes so that the order is random
    random.shuffle(storage_nodes)

    # Create a client socket to send the request to the Storage Nodes
    for storage_node in storage_nodes:
        sock = context.socket(zmq.REQ)
        endpoint = f"tcp://{storage_node['address']}:{storage_node['port_get_data']}"
        logger.info(
            f"Connecting to Storage Node {storage_node['friendly_name']} at {endpoint}"
        )
        sock.connect(endpoint)
        request = protobuf_msgs.Message(
            type=protobuf_msgs.MsgType.GET_DATA_REQUEST,
            get_data_request=protobuf_msgs.GetDataRequest(
                file_uid=file_metadata["uid"]),
        )

        request_serialized = request.SerializeToString()
        sock.send_multipart([request_serialized])

        # Set timeout for receiving the response from the Storage Node
        # sock.RCVTIMEO = time_to_wait(file_metadata["size"])
        response: List[bytes] = sock.recv_multipart()
        msg = protobuf_msgs.Message.FromString(response[0])
        if msg.type != protobuf_msgs.MsgType.GET_DATA_RESPONSE:
            logger.error(
                f"Received invalid response from Storage Node {storage_node['uid']}"
            )
            continue

        if msg.get_data_response.success:
            file_data: bytes = msg.get_data_response.file_data
            content_type: str = file_metadata["content_type"]
            return send_file(io.BytesIO(file_data), mimetype=content_type)

        else:
            logger.error(
                f"Error receiving replica {file_metadata['uid']} from Storage Node {storage_node['uid']}"
            )

        sock.close()

    return make_response({"message": "Error downloading file"}, 500)

@app.route("/files/<int:file_id>/task1.2", methods=["GET"])
def download_file_task1_2(file_id: int) -> Response:
    return redirect(f"/files/{file_id}/task1.1", code=307)


@app.route("/files/<int:file_id>/task2.1", methods=["GET"])
def download_file_task2_1(file_id: int) -> Response:
    """
    Download a file from the storage system using Task 2.1.
    """
    logger.info(f"Received request to download file {file_id} using Task 2.1")

    l: int = args.max_erasures

    # Get the file metadata from the database
    file_metadata = get_file_metadata_from_db(file_id)
    if not file_metadata:
        err_msg = f"File {file_id} not found"
        logger.error(err_msg)
        return make_response({"message": err_msg}, 404)

    db = get_db()

    # Get cursor to get the storage nodes that have the file
    cursor = db.execute(
        """
        SELECT sn.*, f.uid AS fragment_uid
        FROM `storage_nodes` AS sn
        JOIN `fragments` AS f
            ON f.file_metadata_id = ?
        WHERE sn.storage_node_id = f.storage_node_id
        """,
        [file_id],
    )
    
    if not cursor:
        error_msg: str = "Error connecting to the database"
        logger.error(error_msg)
        return make_response({"message": error_msg}, 500)

    # Get the storage nodes that have the file
    rows = cursor.fetchall()
    rows_as_dict = [dict(row) for row in rows]

    # A list of dictionaries, each dictionary containing the `chunkname` and the `data` 
    symbols = []

    for i, row in enumerate(rows_as_dict):
        fragment_uid: str = row["fragment_uid"]
        storage_node_uid: str = row["uid"]
        ipv4_address: str = row["address"]
        port_get_data: int = row["port_get_data"]
        friendly_name: str = row["friendly_name"]

        # Create a client socket to send the request to the Storage Nodes
        sock = context.socket(zmq.REQ)
        endpoint: str = f"tcp://{ipv4_address}:{port_get_data}"
        sock.connect(endpoint)

        # Send the request to the Storage Node
        request = protobuf_msgs.Message(
            type=protobuf_msgs.MsgType.GET_DATA_REQUEST,
            get_data_request=protobuf_msgs.GetDataRequest(
                file_uid=fragment_uid),
        )
        request_serialized = request.SerializeToString()
        sock.send_multipart([request_serialized])

        # Wait for the response from the Storage Node
        response: List[bytes] = sock.recv_multipart()
        try:
            msg = protobuf_msgs.Message.FromString(response[0])
        except protobuf_msgs.DecodeError as e:
            logger.error(f"Error decoding response from Storage Node {friendly_name}")
            return make_response({"message": "Error decoding response"}, 500)
        else:
            if msg.type != protobuf_msgs.MsgType.GET_DATA_RESPONSE:
                logger.error(
                    f"Received invalid response from Storage Node {friendly_name}"
                )
                return make_response({"message": "Invalid response"}, 500)

            if msg.get_data_response.success:
                file_data: bytes = msg.get_data_response.file_data
                symbols.append({
                    "chunkname": fragment_uid,
                    "data": bytearray(file_data)
                })
                logger.info(f"Received replica {fragment_uid} from Storage Node {friendly_name} [{i+1}/{len(rows_as_dict)}]")
                if len(symbols) == constants.TOTAL_NUMBER_OF_STORAGE_NODES - l:
                    logger.info(f"Found enough fragments to reconstruct the file")
                    break
            else:
                logger.error(
                    f"Error receiving replica {fragment_uid} from Storage Node {friendly_name}"
                )
                return make_response({"message": "Error receiving replica"}, 500)

        sock.close()

    # Reconstruct the file
    filesize = file_metadata['size']
    
    filedata_reconstructed: bytes = reedsolomon.decode_file(symbols)[:filesize]

    return send_file(io.BytesIO(filedata_reconstructed), mimetype=file_metadata["content_type"])


@app.route("/files/<int:file_id>/task2.2", methods=["GET"])
def download_file_task2_2(file_id: int) -> Response:
    """
    Download a file from the storage system using Task 2.2.
    """

    logger.info(f"Received request to download file {file_id} using Task 2.2")

    file_metadata = get_file_metadata_from_db(file_id)
    if not file_metadata:
        err_msg = f"File {file_id} not found"
        logger.error(err_msg)
        return make_response({"message": err_msg}, 404)

    db = get_db()

    # Get cursor to get the storage nodes that have the file
    cursor = db.execute(
        """
        SELECT sn.*, f.uid AS fragment_uid
        FROM `storage_nodes` AS sn
        JOIN `fragments` AS f
            ON f.file_metadata_id = ?
        WHERE sn.storage_node_id = f.storage_node_id
        """,
        [file_id],
    )
    
    if not cursor:
        error_msg: str = "Error connecting to the database"
        logger.error(error_msg)
        return make_response({"message": error_msg}, 500)

    # Get the storage nodes that have the file
    rows = cursor.fetchall()
    rows_as_dict = [dict(row) for row in rows]

    l: int = args.max_erasures  # Should probably be stored in the database
    filesize = file_metadata['size']

    msg = protobuf_msgs.Message(
        type=protobuf_msgs.MsgType.GET_FRAGMENTS_AND_DECODE_THEM_REQUEST,
        get_fragments_and_decode_them_request=protobuf_msgs.GetFragmentsAndDecodeThemRequest(
            l=l,
            filesize=filesize,
            fragment_uids_to_storage_nodes = {
                row['fragment_uid']: protobuf_msgs.StorageNode(
                    uid=row['uid'],
                    ipv4=row['address'],
                    port_get_data=row['port_get_data'],
                    port_store_data=row['port_store_data'],
                )
                for row in rows_as_dict
            }
        )
    )

    msg_serialized = msg.SerializeToString()

    # Pick random node
    storage_node = random.choice(rows_as_dict)

    ipv4_addr = storage_node['address']
    port_get_data = storage_node['port_get_data']

    sock = context.socket(zmq.REQ)
    sock.connect(f"tcp://{ipv4_addr}:{port_get_data}")
    sock.send_multipart([msg_serialized])

    # await response
    response = sock.recv_multipart()
    sock.close()

    try:
        msg = protobuf_msgs.Message.FromString(response[0])
    except zmq.error.ZMQError as e:
        logger.error(f"Error receiving message: {e}")
        return make_response({"message": "Error receiving message"}, 500)

    # Assert that the message is of the correct type i.e. GetDataResponse
    if msg.type != protobuf_msgs.MsgType.GET_DATA_RESPONSE:
        logger.error(f"Received message of type {msg.type} instead of GetDataResponse")
        return make_response({"message": "Error receiving message"}, 500)
    
    file_data = msg.get_data_response.file_data
    success = msg.get_data_response.success
    if not success:
        logger.error(f"Error getting file data: {file_data}")
        return make_response({"message": "Error getting file data"}, 500)
    
    return send_file(io.BytesIO(file_data), mimetype=file_metadata["content_type"])



@app.route("/files/<int:file_id>", methods=["GET"])
def download_file(file_id: int) -> Response:
    logger.info(
        f"Received request to download file {file_id}. Storage mode is {args.mode}"
    )

    if args.mode == "task1.1" or args.mode == "task1.2":
        return redirect(f"/files/{file_id}/task1.1", code=307)
    elif args.mode == "task2.1":
        return redirect(f"/files/{file_id}/task2.1", code=307)
    elif args.mode == "task2.2":
        return redirect(f"/files/{file_id}/task2.2", code=307)
    else:
        return make_response({"message": "Invalid mode"}, 400)


    # match args.mode:
    #     case "task1.1" | "task1.2":
    #         return redirect(f"/files/{file_id}/task1.1", code=307)
    #         # The logic for Task 1.2 is the same as Task 1.1
    #     case "task2.1":
    #         return redirect(f"/files/{file_id}/task2.1", code=307)
    #     case "task2.2":
    #         return redirect(f"/files/{file_id}/task2.2", code=307)
    #     case _:
    #         return make_response({"message": "Invalid mode"}, 400)


def get_storage_nodes_from_db() -> List[StorageNode]:
    """
    Returns a list of storage nodes from the database.
    """

    db = get_db()
    cursor = db.execute("SELECT * FROM `storage_nodes`")
    if not cursor:
        return []

    return [StorageNode(**dict(row)) for row in cursor.fetchall()]


def extract_fields_from_post_request(request: Request) -> Tuple[str, str, bytes, int]:
    """
    Extracts the filename, content type and file data from the request.
    """

    payload = request.form

    # Check if request is a form or a json
    if not payload:
        payload = request.get_json()
        if not payload:
            logging.error("No file was uploaded in the request!")
            raise Exception("No file was uploaded in the request!")

    # payload: Any | None = request.get_json()
    filename: str = payload.get("filename")
    content_type: str = payload.get("content_type")
    file_data: bytes = base64.b64decode(payload.get("contents_b64"))
    size = len(file_data)  # The amount of bytes in the file

    return filename, content_type, file_data, size


def time_to_wait(filesize: int) -> int:
    """
    NOTE: NOT USED!
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
        msg = protobuf_msgs.Message(
            type=protobuf_msgs.MsgType.STORE_DATA_REQUEST,
            store_data_request=protobuf_msgs.StoreDataRequest(
                file_uid=str(file_uid), file_data=file_data
            ),
        )
        msg_serialized = msg.SerializeToString()

        sock = context.socket(zmq.REQ)
        sock.connect(f"tcp://{node.address}:{node.port_store_data}")
        sock.send_multipart([msg_serialized])

        return sock

    # Send file to chosen_storage_nodes
    for node in chosen_storage_nodes:
        client: zmq.Socket = send_file_to_node(node)
        logger.debug(f"Sent file to {node.address}:{node.port_store_data}")
        resp = client.recv_multipart()  # await response
        resp_serialized = protobuf_msgs.Message.FromString(resp[0])
        logger.debug(f"Received response: {resp_serialized}")
        client.close()

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

    msg = protobuf_msgs.Message(
        type=protobuf_msgs.MsgType.DELEGATE_STORE_DATA_REQUEST,
        delegate_store_data_request=protobuf_msgs.DelegateStoreDataRequest(
            file_uid=str(file_uid).encode("UTF-8"),
            file_data=file_data,
            nodes_to_forward_to=[
                protobuf_msgs.StorageNode(
                    uid=node.uid.encode("UTF-8"),
                    ipv4=node.address.encode("UTF-8"),
                    port_store_data=node.port_store_data,
                )
                for node in rest_nodes
            ],
        ),
    )

    msg_serialized = msg.SerializeToString()

    # Create a REQ socket to send the request to the first node
    sock = context.socket(zmq.REQ)
    sock.connect(f"tcp://{first_node.address}:{first_node.port_store_data}")
    sock.send_multipart([msg_serialized])

    # Wait for the response
    received = sock.recv_multipart()
    try:
        response = protobuf_msgs.Message.FromString(received[0])
        # response = messages_pb2.DelegateStoreDataResponse.FromString(received[0])
    except protobuf_msgs.DecodeError as e:
        logger.error(f"Error decoding response: {e}")
        return make_response({"error": "Error decoding response"}, 500)
    else:
        if response.type != protobuf_msgs.MsgType.DELEGATE_STORE_DATA_RESPONSE:
            logger.error(f"Unexpected response type: {response.type}")
            return make_response({"error": "Unexpected response type"}, 500)

        logger.info(f"Received response: {response}")
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
            [
                filename,
                filesize,
                content_type,
                str(file_uid),
                "replication_with_delegation",
            ],
        )

        db.commit()

        file_id: int = cursor.lastrowid

        for node in first_k_nodes:
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
    data = bytearray(file_data) # Kodo expects a bytearray

    # Encode the data with REED-SOLOMON
    fragment_names, fragment_data = reedsolomon.store_file(
        data, l
    )

    file_uid = uuid.uuid4()

    def send_file_to_node(node: StorageNode, fragment_name: str, fragment_data: bytes) -> zmq.Socket:
        assert isinstance(fragment_name, str), f"Expected str, got {type(fragment_name)}"
        assert isinstance(fragment_data, bytes), f"Expected bytes, got {type(fragment_data)}"

        msg = protobuf_msgs.Message(
            type=protobuf_msgs.MsgType.STORE_DATA_REQUEST,
            store_data_request=protobuf_msgs.StoreDataRequest(
                file_uid=fragment_name, 
                file_data=fragment_data
            ),
        )
        msg_serialized = msg.SerializeToString()

        sock = context.socket(zmq.REQ)
        sock.connect(f"tcp://{node.address}:{node.port_store_data}")
        sock.send_multipart([msg_serialized])

        return sock

    storage_nodes = get_storage_nodes_from_db()
    assert len(storage_nodes) == constants.TOTAL_NUMBER_OF_STORAGE_NODES, f"Expected {constants.TOTAL_NUMBER_OF_STORAGE_NODES} storage nodes, got {len(storage_nodes)}"


    # Send file to chosen_storage_nodes
    for node, fragment, name in zip(storage_nodes ,fragment_data, fragment_names):
        client: zmq.Socket = send_file_to_node(node,fragment_data=fragment, fragment_name=name)
        logger.debug(f"Sent file to {node.address}:{node.port_store_data}")
        resp = client.recv_multipart()  # await response
        resp_serialized = protobuf_msgs.Message.FromString(resp[0])
        logger.debug(f"Received response: {resp_serialized}")
        client.close()


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
        (filename, filesize, content_type, str(file_uid),  "reedsolomon"),
    )

    db.commit()

    file_id: int = cursor.lastrowid

    for fragment_name, node in zip(fragment_names, storage_nodes):
        db.execute(
            f"""
        INSERT INTO
            `fragments` (
                `file_metadata_id`,
                'storage_node_id',
                'uid'
            )
        VALUES (
            (SELECT file_metadata_id FROM file_metadata WHERE file_metadata_id = ?),
            (SELECT storage_node_id FROM storage_nodes WHERE uid = ?),
            ?
        )
        """,
            (file_id, node.uid, fragment_name),
        )

    db.commit()

    return make_response({"id": file_id}, 201)


@app.route("/files_task2.2", methods=["POST"])
def add_files_task2_2() -> Response:

    filename, content_type, file_data, filesize = extract_fields_from_post_request(
        request
    )

    storage_nodes = get_storage_nodes_from_db()
    assert len(storage_nodes) == constants.TOTAL_NUMBER_OF_STORAGE_NODES, f"Expected {constants.TOTAL_NUMBER_OF_STORAGE_NODES} storage nodes, got {len(storage_nodes)}"

    random.shuffle(storage_nodes)

    node_that_does_the_encoding = storage_nodes[0]
    nodes_that_receive_fragments = storage_nodes[1:]
    
    l: int = args.max_erasures
    file_uid = uuid.uuid4()

    msg = protobuf_msgs.Message(
        type=protobuf_msgs.MsgType.ENCODE_AND_FORWARD_FRAGMENTS_REQUEST,
        encode_and_forward_fragments_request=protobuf_msgs.EncodeAndForwardFragmentsRequest(
            l=l,
            file_uid=str(file_uid).encode("UTF-8"),
            file_data=file_data,
            nodes_to_forward_to=[
                protobuf_msgs.StorageNode(
                    uid=node.uid.encode("UTF-8"),
                    ipv4=node.address.encode("UTF-8"),
                    port_store_data=node.port_store_data,
                )
                for node in nodes_that_receive_fragments
            ],
        ),
    )

    logger.debug(f" {msg}")

    msg_serialized = msg.SerializeToString()


    # Create a REQ socket to send the request to the first node
    sock = context.socket(zmq.REQ)
    sock.connect(f"tcp://{node_that_does_the_encoding.address}:{node_that_does_the_encoding.port_store_data}")
    sock.send_multipart([msg_serialized])

    file_uid = uuid.uuid4()

    # Wait for the response
    received = sock.recv_multipart()

    try:
        response = protobuf_msgs.Message.FromString(received[0])
        # response = messages_pb2.DelegateStoreDataResponse.FromString(received[0])
    except protobuf_msgs.DecodeError as e:
        logger.error(f"Error decoding response: {e}")
        return make_response({"error": "Error decoding response"}, 500)
    else:
        if response.type != protobuf_msgs.MsgType.ENCODE_AND_FORWARD_FRAGMENTS_RESPONSE:
            logger.error(f"Unexpected response type: {response.type}")
            return make_response({"error": "Unexpected response type"}, 500)

        logger.info(f"Received response: {response}")

        fragment_uids_to_storage_nodes = response \
            .encode_and_forward_fragments_response \
            .fragment_uids_to_storage_nodes

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
            [
                filename,
                filesize,
                content_type,
                str(file_uid),
                "reedsolomon_with_delegation",
            ],
        )

        db.commit()

        file_id: int = cursor.lastrowid

        for fragment_uid, node in fragment_uids_to_storage_nodes.items():
            # print(f"fragment_uid: {fragment_uid}")
            # print(f"node: {node}")

            db.execute(
                f"""
            INSERT INTO
                `fragments` (
                    `file_metadata_id`,
                    'storage_node_id',
                    'uid'
                )
            VALUES (
                (SELECT file_metadata_id FROM file_metadata WHERE file_metadata_id = ?),
                (SELECT storage_node_id FROM storage_nodes WHERE uid = ?),
                ?
            )
            """,
                (file_id, node.uid, fragment_uid),
            )

        db.commit()
        logger.info(f"Inserted file with id {file_id} into database")
        return make_response({"id": cursor.lastrowid}, 201)
    finally:
        sock.close()



@app.route("/files", methods=["POST"])
def add_files() -> Response:
    """
    Add a new file to the storage system.
    """

    logger.info(f"mode: {args.mode}")
    if args.mode == "task1.1":
        return redirect("/files_task1.1", code=307)
    elif args.mode == "task1.2":
        return redirect("/files_task1.2", code=307)
    elif args.mode == "task2.1":
        return redirect("/files_task2.1", code=307)
    elif args.mode == "task2.2":
        return redirect("/files_task2.2", code=307)
    else:
        return make_response("Wrong storage mode", 400)

    # match args.mode:
    #     case "task1.1":
    #         return redirect("/files_task1.1", code=307)
    #     case "task1.2":
    #         return redirect("/files_task1.2", code=307)
    #     case "task2.1":
    #         return redirect("/files_task2.1", code=307)
    #     case "task2.2":
    #         return redirect("/files_task2.2", code=307)
    #     case _:
    #         return make_response("Wrong storage mode", 400)



@app.errorhandler(500)
def server_error(e) -> Response:
    logger.exception("Internal error: %s", e)
    return make_response({"error": str(e)}, 500)



# -- SETUP -- #
sock_pull_storage_node_advertisement = context.socket(zmq.REP)
sock_pull_storage_node_advertisement.bind(
    f"tcp://*:{constants.PORT_STORAGE_NODE_ADVERTISEMENT}"
)

for i in range(constants.TOTAL_NUMBER_OF_STORAGE_NODES):
    logger.debug(
        f"Waiting for storage node advertisement ... [{i+1}/{constants.TOTAL_NUMBER_OF_STORAGE_NODES}]"
    )

    # Receive the advertisement from the storage node
    msg = sock_pull_storage_node_advertisement.recv()  # TODO: add timeout
    # Expect msg to be of type StorageNodeAdvertisement
    advertisement = protobuf_msgs.StorageNodeAdvertisementRequest()
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

    cursor = db.execute(
        """
        SELECT * FROM `storage_nodes` WHERE `uid`= ? AND `address`=? AND `port_get_data`=?
    """,
        [str(uid), ipv4_addr, port_get_data],
    )

    # If the storage node is not in the database, add it
    if not cursor.fetchone():
        db.execute(
            """
        INSERT INTO `storage_nodes`(`uid`, `friendly_name`, `address`, `port_get_data`, `port_store_data`) VALUES (?,?,?,?,?)
        """,
            (str(uid), friendly_name, ipv4_addr, port_get_data, port_store_data),
        )
        db.commit()

    # Send the response to the storage node
    resp = protobuf_msgs.StorageNodeAdvertisementResponse()
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
