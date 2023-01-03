import argparse
import uuid
import os
import pathlib
from pathlib import Path
import shutil
import sys
import time
import reedsolomon
import zmq
from typing import Optional

import messages_pb2 as protobuf_msgs

from utils import is_raspberry_pi, create_logger, get_interface_ipaddress
import constants

name_of_this_script = os.path.basename(__file__).split(".")[0]


parser = argparse.ArgumentParser(
    prog=name_of_this_script,
    description="Storage node"
)

parser.add_argument(
    "data_folder", type=str, help="Folder where chunks should be stored"
)

parser.add_argument(
    'master_ip_addr', type=str, help="IP address of the master node"
)

args = parser.parse_args()

# logger = create_logger(name_of_this_script)
logger = create_logger(args.data_folder)
logger.info(f"log level is {logger.getEffectiveLevel()}")


# Check if folder exists, else create it
if not os.path.exists(args.data_folder):
    logger.info(f"Folder {args.data_folder} does not exist, creating it")
    os.makedirs(args.data_folder)
else:
    assert os.path.isdir(args.data_folder), f"{args.data_folder} is not a folder"


DATA_FOLDER = pathlib.Path(args.data_folder)


MAX_CHUNKS_PER_FILE = 10

# Check whether the node has an id. If it doesn't, generate one and save it to disk.
try:
    filename = DATA_FOLDER / ".node_id"
    with open(filename, "r") as id_file:
        NODE_UID = id_file.read()
        # Remove trailing newline
        NODE_UID = NODE_UID.strip()
        # Convert to UUID
        NODE_UID = uuid.UUID(NODE_UID)
        logger.info(f"ID read from file: {NODE_UID}")
except FileNotFoundError:
    # This is OK, this must be the first time the node was started
    NODE_UID: uuid.UUID = uuid.uuid4()
    # Save it to file for the next start
    with open(filename, "w") as id_file:
        id_file.write(str(NODE_UID))
        logger.info(f"New ID generated and saved to file: {NODE_UID}")


if is_raspberry_pi():
    ip_address: str = get_interface_ipaddress("eth0")
    logger.info(f"Running on Raspberry Pi, using eth0 addr={ip_address}")
    assert "192.168" in ip_address, "IP address must start with 192.168."

    pull_address = f"tcp://{ip_address}:5557"
    
    sender_address = f"tcp://{ip_address}:5558"
    subscriber_address = f"tcp://{ip_address}:5559"
    repair_subscriber_address = f"tcp://{ip_address}:5560"
    repair_sender_address = f"tcp://{ip_address}:5561"
    server_sock_task_1_2_address = f"tcp://{ip_address}:5562"
    sock_router_heartbeat_request_addr = f"tcp://{ip_address}:{constants.PORT_HEARTBEAT}"
else:
    # On the local computer: use localhost
    pull_address = "tcp://localhost:5557"
    push_address = "tcp://localhost:5558"
    subscriber_address = "tcp://localhost:5559"
    repair_subscriber_address = "tcp://localhost:5560"
    repair_sender_address = "tcp://localhost:5561"
    server_sock_task_1_2_address = f"tcp://localhost:5562"
    sock_router_heartbeat_request_addr = f"tcp://localhost:{constants.PORT_HEARTBEAT}"


context = zmq.Context()
logger.info("Created ZMQ context")

# Socket to receive Store Chunk messages from the controller
# receiver = context.socket(zmq.PULL)
# receiver.connect(pull_address)
# logger.info(f"Created zmq.PULL socket and connected to {pull_address}")

# Socket to send results to the controller
# sender = context.socket(zmq.PUSH)
# sender.connect(push_address)
# logger.info(f"Created zmq.PUSH socket and connected to {push_address}")

# # Socket to receive Get Chunk messages from the controller
# subscriber = context.socket(zmq.SUB)
# subscriber.connect(subscriber_address)


# # Receive every message (empty subscription)
# subscriber.setsockopt(zmq.SUBSCRIBE, b"")
# logger.info(f"Created zmq.SUB socket and connected to {subscriber_address}")

# Socket to receive Repair request messages from the controller
# repair_subscriber = context.socket(zmq.SUB)
# repair_subscriber.connect(repair_subscriber_address)
# logger.info(f"Created zmq.SUB socket and connected to {repair_subscriber_address}")
# # Receive messages destined for all nodes


# repair_subscriber.setsockopt(zmq.SUBSCRIBE, b"all_nodes")
# # Receive messages destined for this node
# repair_subscriber.setsockopt(zmq.SUBSCRIBE, NODE_UID.bytes)


# # Socket to send repair results to the controller
# repair_sender = context.socket(zmq.PUSH)
# repair_sender.connect(repair_sender_address)
# logger.info(f"Created zmq.PUSH socket and connected to {repair_sender_address}")

# subscriber_sock_task1_2 = context.socket(zmq.SUB)
# subscriber_sock_task1_2.setsockopt(zmq.SUBSCRIBE, b"all_nodes")
# subscriber_sock_task1_2.connect(server_sock_task_1_2_address)

# sock_router_heartbeat_request = context.socket(zmq.ROUTER)
# sock_router_heartbeat_request.bind(sock_router_heartbeat_request_addr)
# sock_sub_heartbeat_request.setsocketopt(zmq.SUBSCRIBE, constants.TOPIC_HEARTBEAT.encode("utf-8"))


sock_rep_store_data = context.socket(zmq.REP)
# Get random port that is not in use
sock_rep_store_data.bind_to_random_port("tcp://*", min_port=5000, max_port=6999)
PORT_STORE_DATA: int = int(sock_rep_store_data.getsockopt(zmq.LAST_ENDPOINT).decode("utf-8").split(":")[-1])
logger.info(f"Created zmq.REP socket to handle `store data` and bound to {PORT_STORE_DATA}")

sock_rep_get_data = context.socket(zmq.REP)
# Get random port that is not in use
sock_rep_get_data.bind_to_random_port("tcp://*", min_port=5000, max_port=5999)
PORT_GET_DATA: int = int(sock_rep_get_data.getsockopt(zmq.LAST_ENDPOINT).decode("utf-8").split(":")[-1])
logger.info(f"Created zmq.REP socket to handle `get data` and bound to {PORT_GET_DATA}")


# Use a Poller to monitor multiple sockets at the same time
poller = zmq.Poller()
for sock in [
    # receiver,
    # subscriber,
    # repair_subscriber,
    sock_rep_get_data,
    sock_rep_store_data
    # sock_router_heartbeat_request
]:
    poller.register(sock, zmq.POLLIN)

# poller.register(receiver, zmq.POLLIN)
# poller.register(subscriber, zmq.POLLIN)
# poller.register(repair_subscriber, zmq.POLLIN)


def receiver_action(subscriber: zmq.Socket, sender: zmq.Socket) -> None:
    assert (
        subscriber is not None and sender is not None
    ), "subscriber_action: subscriber and sender must not be None"
    assert (
        subscriber.type == zmq.SUB
    ), "subscriber_action: subscriber must be a SUB socket"
    assert sender.type == zmq.PUSH, "subscriber_action: sender must be a PUSH socket"

    try:
        # Incoming message on the 'receiver' socket where we get tasks to store a chunk
        msg = receiver.recv_multipart()
    except zmq.ZMQError as e:
        logger.error(f"Error receiving message: {e}")
        return

    # Parse the Protobuf message from the first frame
    task = protobuf_msgs.StoreDataRequest()
    logger.debug(f"task: {task}")
    
    task.ParseFromString(msg[1])
    
    with open(f"{DATA_FOLDER}/{task.file_uid}", "wb") as f:
        f.write(task.file_data)
        logger.info(f"File {task.file_uid} saved to {DATA_FOLDER}")

    # Send response (just the file name)
    sender.send_string(task.file_uid)


# This function is called when a message is received on the subscriber socket


def subscriber_action(subscriber: zmq.Socket, sender: zmq.Socket) -> None:
    # Incoming message on the 'subscriber' socket where we get retrieve requests
    msg = subscriber.recv()

    # Parse the Protobuf message from the first frame
    task = protobuf_msgs.GetDataRequest()
    task.ParseFromString(msg)

    file_uid = task.file_uid
    logger.info(f"Get data request: {file_uid}")

    # check if the file exists
    # f: Path = DATA_FOLDER / file_uid
    if not (DATA_FOLDER / file_uid).exists():
        logger.error(f"File {file_uid} not found")
        return
    
    # Read the file and send it back
    with open(f"{DATA_FOLDER}/{file_uid}", "rb") as f:
        logger.info(f"Sending file {file_uid} back")

        sender.send_multipart([bytes(file_uid, 'utf-8'), f.read()])

    
    # # Try to load all fragments with this name
    # # First frame is the filename
    # frames = [bytes(file_uid, "utf-8")]
    # # Subsequent frames will contain the chunks' data

    # # iterate over all files in args.data_folder
    # for i, file in enumerate(DATA_FOLDER.glob("*")):
    #     logger.debug(f"Found file [{i}] {file.name}")
    #     if file.is_file() and file.name == file_uid:
    #         logger.info(f"Found chunk {file_uid}, sending it back")
    #         frames.append(file.read_bytes())

    # # Only send a result if at least one chunk was found
    # if len(frames) > 1:
    #     logger.info(f"Sending {len(frames) - 1} chunks back")
    #     sender.send_multipart(frames)


def nuke_storage_folder() -> None:
    if os.environ.get("DEBUG") is not None:
        logger.info(f"DEBUG mode, deleting storage folder: {DATA_FOLDER}")
        shutil.rmtree(DATA_FOLDER)


# atexit.register(nuke_storage_folder)


IPV4_ADDR: str = get_interface_ipaddress('eth0') if is_raspberry_pi() else 'localhost'

def setup(master_node_addr: str, attempts: int = 10, timeout_between_attempts: int = 1) -> None:
    """
    Send a broadcast message to the master node, and the other storage nodes
    containing this storage nodes UID, aswell as its IPv4 address.
    """

    sock = context.socket(zmq.REQ)
    sock.connect(f"tcp://{master_node_addr}:{constants.PORT_STORAGE_NODE_ADVERTISEMENT}")
    for i in range(attempts):
        # Create a new message
        msg = protobuf_msgs.StorageNodeAdvertisementRequest()
        msg.node.uid = str(NODE_UID).encode("utf-8")
        logger.debug(f"Using IPv4 address: {IPV4_ADDR}")
        msg.node.ipv4 = IPV4_ADDR
        # Get a random port for the storage node to listen on, that is not in use
        # msg.node.port = PORT_GET_DATA
        msg.node.port_get_data = PORT_GET_DATA
        msg.node.port_store_data = PORT_STORE_DATA

        msg.friendly_name = DATA_FOLDER.name
    
        # Send the message
        sock.send(msg.SerializeToString())

        # Wait for the master node to acknowledge the message
        response = sock.recv_multipart()
        storage_node_advertisement_response = protobuf_msgs.StorageNodeAdvertisementResponse()
        storage_node_advertisement_response.ParseFromString(response[0])
        if storage_node_advertisement_response.success:
            logger.info(f"Successfully registered with master node: {storage_node_advertisement_response}")
            sock.close()
            return
        else:
            logger.warning(f"Failed to register with master node: {storage_node_advertisement_response}")
            time.sleep(timeout_between_attempts)
            
    sock.close()
    raise Exception("Failed to register with master node")


def get_data_action(sock_rep_get_data: zmq.Socket) -> None:
    assert sock_rep_get_data is not None and sock_rep_get_data.type == zmq.REP, f"sock_rep_get_data is not a REP socket, but a {sock_rep_get_data.type}"
    logger.info("Received message on sock_rep_get_data socket")
    received = sock_rep_get_data.recv_multipart()
    try:
        request = protobuf_msgs.Message.FromString(received[0])
    except protobuf_msgs.DecodeError as e:
        logger.error(f"Failed to parse Message: {e}")
        return

    if request.type == protobuf_msgs.MsgType.GET_DATA_REQUEST:
    # match request.type:
        # case protobuf_msgs.MsgType.GET_DATA_REQUEST:
        assert request.WhichOneof("payload") in ["get_data_request", "delegate_get_data_request"], f"Message type is GET_DATA_REQUEST, but payload is not a GetDataRequest, but a {request.WhichOneof('payload')}"
        get_data_request = request.get_data_request
        logger.info(f"Received request for file {get_data_request.file_uid}")
        # Check if the file exists
        f: Path = DATA_FOLDER / get_data_request.file_uid
        if not f.exists():
            logger.error(f"File {get_data_request.file_uid} not found")
            response = protobuf_msgs.Message(
                type=protobuf_msgs.MsgType.GET_DATA_RESPONSE,
                get_data_response=protobuf_msgs.GetDataResponse(
                    success=False
                )
            )
            sock_rep_get_data.send_multipart([response.SerializeToString()])
        else:
            # Read the file and send it back
            with open(f"{DATA_FOLDER}/{get_data_request.file_uid}", "rb") as f:
                logger.info(f"Sending file {get_data_request.file_uid} back")
                response = protobuf_msgs.Message(
                    type=protobuf_msgs.MsgType.GET_DATA_RESPONSE,
                    get_data_response=protobuf_msgs.GetDataResponse(
                        success=True,
                        file_data=f.read()
                    )
                )
                sock_rep_get_data.send_multipart([response.SerializeToString()])


        # TASK 2.2
    elif request.type == protobuf_msgs.MsgType.GET_FRAGMENTS_AND_DECODE_THEM_REQUEST:
        # case protobuf_msgs.MsgType.GET_FRAGMENTS_AND_DECODE_THEM_REQUEST:
        assert request.WhichOneof("payload") == "get_fragments_and_decode_them_request", f"Message type is GET_FRAGMENTS_AND_DECODE_THEM_REQUEST, but payload is not a GetFragmentsAndDecodeThemRequest, but a {request.WhichOneof('payload')}"
        get_fragments_and_decode_them_request = request.get_fragments_and_decode_them_request
        
        logger.info(f"This node was selected to be in charge of encoding the file with Reed-Solomon and forward fragments to the 3 other nodes in the storage system.")

        l: int = get_fragments_and_decode_them_request.l
        filesize: int = get_fragments_and_decode_them_request.filesize
        fragment_uids_to_storage_nodes = get_fragments_and_decode_them_request.fragment_uids_to_storage_nodes

        symbols = []

        # Extract the storage node, that matches this one            
        fragment_uid_meant_for_this_storage_node: Optional[str] = None

        for fragment_uid, storage_node in fragment_uids_to_storage_nodes.items():
            logger.debug(f"Checking if {storage_node.uid} is {NODE_UID}")
            if storage_node.uid == str(NODE_UID):
                logger.info(f"Found fragment meant for this storage node: {fragment_uid}")
                fragment_uid_meant_for_this_storage_node = fragment_uid
                break
        
        if fragment_uid_meant_for_this_storage_node is None:
            logger.error(f"Could not find fragment meant for this storage node")
            sys.exit(1)

        f = DATA_FOLDER / fragment_uid_meant_for_this_storage_node
        if not f.exists():
            logger.error(f"Fragment {fragment_uid_meant_for_this_storage_node} not found")
            sys.exit(1)

        with open(f, "rb") as f:
            data = f.read()

        symbols.append({
            "chunkname": fragment_uid_meant_for_this_storage_node,
            "data": bytearray(data)
        })

        # Get the fragments from the storage nodes
        fragment_uids_to_storage_nodes_filtered = {
            k: v
            for k, v in fragment_uids_to_storage_nodes.items()
            if k != fragment_uid_meant_for_this_storage_node
        }

        assert len(fragment_uids_to_storage_nodes_filtered) == len(fragment_uids_to_storage_nodes) - 1, f"fragment_uids_to_storage_nodes_filtered has {len(fragment_uids_to_storage_nodes_filtered)} elements, but fragment_uids_to_storage_nodes has {len(fragment_uids_to_storage_nodes)} elements"
        
        # Remove l elements from the dict
        fragment_uids_to_storage_nodes_filtered = dict(list(fragment_uids_to_storage_nodes_filtered.items())[l:])

        for fragment_uid, storage_node in fragment_uids_to_storage_nodes_filtered.items():
            logger.info(f"Getting fragment {fragment_uid} from storage node {storage_node}")
            # Create a socket to connect to the storage node
            sock = context.socket(zmq.REQ)
            sock.connect(f"tcp://{storage_node.ipv4}:{storage_node.port_get_data}")
            # Send the request
            msg = protobuf_msgs.Message(
                type=protobuf_msgs.MsgType.GET_DATA_REQUEST,
                get_data_request=protobuf_msgs.GetDataRequest(
                    file_uid=fragment_uid
                )
            )
            sock.send_multipart([msg.SerializeToString()])

            # Wait for the response
            response = sock.recv_multipart()
            response_msg = protobuf_msgs.Message.FromString(response[0])
            assert response_msg.WhichOneof("payload") == "get_data_response", f"Message type is GET_DATA_RESPONSE, but payload is not a GetDataResponse, but a {response_msg.WhichOneof('payload')}"
            response_get_data = response_msg.get_data_response
            assert response_get_data.success, f"Failed to get fragment {fragment_uid} from storage node {storage_node}"

            symbol = {
                "chunkname": fragment_uid,
                "data": bytearray(response_get_data.file_data)
            }
            symbols.append(symbol)
            
            # Close the socket
            sock.close()

    
        filedata_return: bytearray = reedsolomon.decode_file(
            symbols 
        )[:filesize]


        # Create the final response to the rest-client
        response = protobuf_msgs.Message(
            type=protobuf_msgs.MsgType.GET_DATA_RESPONSE,
            get_data_response=protobuf_msgs.GetDataResponse(
                success=True,
                file_data=bytes(filedata_return)
            )
        )

        response_serialized = response.SerializeToString()
        sock_rep_get_data.send_multipart([response_serialized])
        logger.info(f"Sent file back to client :-D")
    else:
        # case _:
        logger.error(f"Received unknown message type: {request.type}")


def store_data_action() -> None:
    assert sock_rep_store_data is not None and sock_rep_store_data.type == zmq.REP, f"sock_rep_store_data is not a REP socket, but a {sock_rep_store_data.type}"
    logger.info("Received message on sock_rep_store_data socket")
    received = sock_rep_store_data.recv_multipart()
    try:
        request = protobuf_msgs.Message.FromString(received[0])
    except protobuf_msgs.DecodeError as e:
        logger.error(f"Failed to parse Message: {e}")
        return
    
    if request.type == protobuf_msgs.MsgType.STORE_DATA_REQUEST:
    # match request.type:
        # TASK 1.1
        # case protobuf_msgs.MsgType.STORE_DATA_REQUEST:
        assert request.WhichOneof("payload") == "store_data_request", f"Message type is STORE_DATA_REQUEST, but payload is not a StoreDataRequest, but a {request.WhichOneof('payload')}"
        store_data_request = request.store_data_request
        logger.info(f"Received request to store file {store_data_request.file_uid}")

        f = DATA_FOLDER / store_data_request.file_uid
        success: bool = False
        if f.exists():
            logger.error(f"File {store_data_request.file_uid} already exists")
            success = False

        else:
            # Write the file
            with open(f"{DATA_FOLDER}/{store_data_request.file_uid}", "wb") as f:
                f.write(store_data_request.file_data)
                logger.info(f"Stored file {store_data_request.file_uid}")
                success = True

        response = protobuf_msgs.Message(
                type=protobuf_msgs.MsgType.STORE_DATA_RESPONSE,
                store_data_response=protobuf_msgs.StoreDataResponse(
                    success=success
                )
            )
        sock_rep_store_data.send_multipart([response.SerializeToString()])  

        # TASK 1.2
    elif request.type == protobuf_msgs.MsgType.DELEGATE_STORE_DATA_REQUEST:
        # case protobuf_msgs.MsgType.DELEGATE_STORE_DATA_REQUEST:

        assert request.WhichOneof("payload") == "delegate_store_data_request", f"Message type is DELEGATE_STORE_DATA_REQUEST, but payload is not a DelegateStoreDataRequest, but a {request.WhichOneof('payload')}"
        delegate_store_data_request = request.delegate_store_data_request
        #logger.info(f"Received request to delegate store file {delegate_store_data_request.file_uid} to node {delegate_store_data_request.node_id}")

        f = DATA_FOLDER / delegate_store_data_request.file_uid
        success: bool = False
        if f.exists():
            logger.error(f"File {delegate_store_data_request.file_uid} already exists")
            success = False
            sys.exit(1)
        
        else:
            t_replication_start: float = time.time()

            # Write the file
            with open(f"{DATA_FOLDER}/{delegate_store_data_request.file_uid}", "wb") as f:
                f.write(delegate_store_data_request.file_data)
                logger.info(f"Stored file {delegate_store_data_request.file_uid}")
                success = True

            # Get the nodes to forward the request to
            nodes_to_forward_to = delegate_store_data_request.nodes_to_forward_to
            # Get the head of the list

            
            if len(nodes_to_forward_to) == 0:
                response = protobuf_msgs.Message(
                    type=protobuf_msgs.MsgType.DELEGATE_STORE_DATA_RESPONSE,
                    delegate_store_data_response=protobuf_msgs.DelegateStoreDataResponse(
                        success=success,
                        time_replication=-1 # Not defined so we set it to -1
                    )
                )

                response_serialized = response.SerializeToString()
                sock_rep_store_data.send_multipart([response_serialized])
                logger.info(f"Last node in the list, sending response back to client")
                
            else:
                head, *tail = nodes_to_forward_to

                first_node_contacted: bool = len(nodes_to_forward_to) == constants.TOTAL_NUMBER_OF_STORAGE_NODES

                if first_node_contacted:
                    t_replication_start: float = time.time()

                # Create the message to forward
                message_to_forward = protobuf_msgs.Message(
                    type=protobuf_msgs.MsgType.DELEGATE_STORE_DATA_REQUEST,
                    delegate_store_data_request=protobuf_msgs.DelegateStoreDataRequest(
                        file_uid=delegate_store_data_request.file_uid,
                        file_data=delegate_store_data_request.file_data,
                        nodes_to_forward_to=tail
                    )
                )
                message_to_forward_serialized = message_to_forward.SerializeToString()
                # Send the message to the head of the list
                sock = context.socket(zmq.REQ)
                sock.connect(f"tcp://{head.ipv4}:{head.port_store_data}")
                sock.send_multipart([message_to_forward_serialized])
                logger.info(f"Forwarded request to delegate store file {delegate_store_data_request.file_uid} to node {head.uid}, waiting for response...")

                # Receive the response
                received = sock.recv_multipart()
                logger.info(f"Received response from node {head.uid}")

                try:
                    response = protobuf_msgs.Message.FromString(received[0])
                except protobuf_msgs.DecodeError as e:
                    logger.error(f"Failed to parse Message: {e}")
                    sys.exit(1)
                else:
                    if response.type == protobuf_msgs.MsgType.DELEGATE_STORE_DATA_RESPONSE:
                        assert response.WhichOneof("payload") == "delegate_store_data_response", f"Message type is DELEGATE_STORE_DATA_RESPONSE, but payload is not a DelegateStoreDataResponse, but a {response.WhichOneof('payload')}"
                        delegate_store_data_response = response.delegate_store_data_response
                        logger.info(f"Received response to delegate store file {delegate_store_data_request.file_uid} to node {head.uid}")
                        if delegate_store_data_response.success:
                            logger.info(f"Successfully delegated store file {delegate_store_data_request.file_uid} to node {head.uid}")
                            # Send the response to the client

                            if first_node_contacted:
                                time_replication = time.time() - t_replication_start
                            else:
                                # If not the first node contacted, then the time_replication is not defined, and we set it to -1
                                time_replication = -1

                            response = protobuf_msgs.Message(
                                type=protobuf_msgs.MsgType.DELEGATE_STORE_DATA_RESPONSE,
                                delegate_store_data_response=protobuf_msgs.DelegateStoreDataResponse(
                                    success=True,
                                    time_replication=time_replication
                                )
                            )
                            response_serialized = response.SerializeToString()
                            sock_rep_store_data.send_multipart([response_serialized])
                        else:
                            logger.error(f"Failed to delegate store file {delegate_store_data_request.file_uid} to node {head.uid}")
                            sys.exit(1)
                    else:
                        logger.error(f"Received unknown message type: {response.type}")
                        sys.exit(1)    

    # TASK 2.2
    elif request.type == protobuf_msgs.MsgType.ENCODE_AND_FORWARD_FRAGMENTS_REQUEST:
        # case protobuf_msgs.MsgType.ENCODE_AND_FORWARD_FRAGMENTS_REQUEST:

        # Welcome message
        logger.info(f"--- Hello. Welcome to my encoding server using Reed-Solomon :)) ---")

        # Parse the message
        assert request.WhichOneof("payload") == "encode_and_forward_fragments_request", f"Message type is ENCODE_AND_FORWARD_FRAGMENTS_REQUEST, but payload is not a EncodeAndForwardFragmentsRequest, but a {request.WhichOneof('payload')}"
        encode_and_forward_fragments_request = request.encode_and_forward_fragments_request
        
        # Get filename and data
        data = bytearray(encode_and_forward_fragments_request.file_data)

        # Get the nodes to forward the request to
        nodes_to_forward_to = encode_and_forward_fragments_request.nodes_to_forward_to
        assert len(nodes_to_forward_to) == 3, f"Must forward to 3 nodes, but forwarding to {len(nodes_to_forward_to)}"
        l = encode_and_forward_fragments_request.l

        assert l in [1, 2], f"l must be 1 or 2, but is {l}"

        # Encode the data with reed solomon
        # fragment_names : list of str (fragment uids)
        # fragment_data  : list of fragment data bytes
        t_start_rs: float = time.time()
        fragment_names, fragment_data = reedsolomon.store_file(data, l)
        t_end_rs: float = time.time()
        t_diff_rs: float = t_end_rs - t_start_rs
        logger.debug(f"Encoding time: {t_diff_rs} seconds")
        # Sending the encoded data to the other storage nodes
        # Create the message to forward

        socks = []

        # print(nodes_to_forward_to)

        for f_name, f_data, storage_node in zip(fragment_names[1:], fragment_data[1:], nodes_to_forward_to):
            # assert isinstance(f_name, str), f"Fragment name must be a string, but is {type(f_name)}"
            # assert isinstance(f_data, bytes), f"Fragment data must be bytes, but is {type(f_data)}"
            # assert isinstance(storage_node, protobuf_msgs.StorageNode), f"Storage node must be a StorageNode, but is {type(storage_node)}"
            message = protobuf_msgs.Message(
                type=protobuf_msgs.MsgType.STORE_DATA_REQUEST,
                store_data_request=protobuf_msgs.StoreDataRequest(
                    file_uid=f_name,
                    file_data=f_data
                )
            )

            # Serialize the message
            message_serialized = message.SerializeToString()

            # Send the message to the storage node
            sock = context.socket(zmq.REQ)
            sock.connect(f"tcp://{storage_node.ipv4}:{storage_node.port_store_data}")
            sock.send_multipart([message_serialized])
            logger.info(f"Forwarded request to store file {f_name} to node {storage_node.uid}, waiting for response...")
            socks.append(sock)

        # Store the fragment for this storage node locally, while awaiting the response from the other nodes
        f = DATA_FOLDER / fragment_names[0]
        if f.exists():
            logger.error(f"File {f} already exists")
            sys.exit(1)
        else:
            f.write_bytes(fragment_data[0])
            logger.info(f"Successfully stored file {f}")
        
        # Make sure the other nodes have stored the data successfully
        for i, sock in enumerate(socks):
            # Receive the response
            received = sock.recv_multipart()
            response = protobuf_msgs.Message.FromString(received[0])
            assert response.WhichOneof("payload") == "store_data_response", f"Message type is STORE_DATA_RESPONSE, but payload is not a StoreDataResponse, but a {response.WhichOneof('payload')}"
            store_data_response = response.store_data_response
            if store_data_response.success:
                logger.info(f"Successfully stored file [{i+1}/3]")
            else:
                logger.error(f"Failed to store file [{i+1}/3]")
                sys.exit(1)
    
            sock.close()
        
        
        # Send the response to the rest-server
        response = protobuf_msgs.Message(
            type=protobuf_msgs.MsgType.ENCODE_AND_FORWARD_FRAGMENTS_RESPONSE,
            encode_and_forward_fragments_response=protobuf_msgs.EncodeAndForwardFragmentsResponse(
                fragment_uids_to_storage_nodes = {
                    uid: storage_node
                    for uid, storage_node in zip(
                        fragment_names,
                        [protobuf_msgs.StorageNode(
                            uid=str(NODE_UID).encode('utf-8'),
                            ipv4=IPV4_ADDR,
                            port_store_data=PORT_STORE_DATA,
                            port_get_data=PORT_GET_DATA

                        ) ,*nodes_to_forward_to]
                    )
                },
                time_rs_encode=t_diff_rs
            )
        )

        response_serialized = response.SerializeToString()

        sock_rep_store_data.send_multipart([response_serialized])

        logger.info("Sent response to rest-server")

    else:
        # case _:
        logger.error(f"Received unknown message type: {request.type}")
        sys.exit(1)

def main_loop() -> None:
    
    # Main loop -----------------------------------------------------------------------------------------
    while True:
        try:
            # Poll all sockets
            socks = dict(poller.poll())
        except KeyboardInterrupt:
            break

        # At this point one or multiple sockets may have received a message

        # if receiver in socks:
        #     # Incoming message on the 'receiver' socket where we get tasks to store a chunk
        #     logger.info("Received message on receiver socket")
        #     sys.exit(1)
        #     # receiver_action(subscriber, sender)

        # if subscriber in socks:
        #     logger.info("Received message on subscriber socket")
        #     sys.exit(1)
            # subscriber_action(subscriber, sender)

        if sock_rep_get_data in socks:
            get_data_action(sock_rep_get_data)

        if sock_rep_store_data in socks:
            store_data_action()

    

if __name__ == '__main__':
    setup(args.master_ip_addr)
    # setup("127.0.0.1")
    logger.info("Starting main loop")
    main_loop()
