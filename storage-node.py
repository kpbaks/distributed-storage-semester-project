import zmq
import messages_pb2
import sys
import os
import random
import string
from utils import random_string, write_file, is_raspberry_pi


context = zmq.Context()
# Socket to receive Store Chunk messages from the controller
pull_address = "tcp://localhost:5557"
receiver = context.socket(zmq.PULL)
receiver.connect(pull_address)
print(f"Listening on {pull_address}")
# Socket to send results to the controller
sender = context.socket(zmq.PUSH)
sender.connect("tcp://localhost:5558")
# Socket to receive Get Chunk messages from the controller
subscriber = context.socket(zmq.SUB)
subscriber.connect("tcp://localhost:5559")
# Receive every message (empty subscription)
subscriber.setsockopt(zmq.SUBSCRIBE, b'')

poller = zmq.Poller()
poller.register(receiver, zmq.POLLIN)
poller.register(subscriber, zmq.POLLIN)

# Read the folder name where chunks should be stored from the first program argument
# (or use the current folder if none was given)
data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"
if data_folder != "./":
# Try to create the folder
    try:
        os.mkdir('./'+data_folder)
    except FileExistsError as _:
# OK, the folder exists
        pass

print(f"Data folder: {data_folder}")


while True:
    try:
# Poll all sockets
        socks = dict(poller.poll())

    except KeyboardInterrupt:
        break
# At this point one or multiple sockets have received a message
    if receiver in socks:
# Incoming message on the 'receiver' socket where we get tasks to store a chunk
        msg = receiver.recv_multipart()
        # Parse the Protobuf message from the first frame
        task = messages_pb2.storedata_request()
        task.ParseFromString(msg[0])
        # The data is the second frame
        import time
        #start_time = time.time()
        data = msg[1]
        print(f"File to save: {task.filename}, size: {len(data)} bytes")
        # Store the chunk with the given filename
        chunk_local_path = data_folder+'/'+task.filename
        write_file(data, chunk_local_path)
        print(f"File saved to {chunk_local_path}")
        #print(f"Time to save: {time.time() - start_time}")
        # Send response (just the file name)
        sender.send_string(task.filename)

    if subscriber in socks:
        # Incoming message on the 'subscriber' socket where we get retrieve requests
        msg = subscriber.recv()
        # Parse the Protobuf message from the first frame
        task = messages_pb2.getdata_request()
        task.ParseFromString(msg)
        filename = task.filename
        print(f"Data chunk request: {filename}")
        # Try to load the requested file from the local file system,
        # send response only if found
        try:
            with open(data_folder+'/'+filename, "rb") as in_file:
                print(f"Found chunk {filename}, sending it back")
                
                sender.send_multipart([
                    bytes(filename, 'utf-8'),
                    in_file.read()
                ])
        except FileNotFoundError:
        # The chunk is not stored by this node
            pass