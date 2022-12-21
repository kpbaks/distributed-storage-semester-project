import math
import random
import sys
from pprint import pp
from typing import List, Tuple

import zmq

import messages_pb2
from storage_provider import StorageProvider
from utils import random_string


class Raid1StorageProvider(StorageProvider):
    """
    Implements a storage provider that uses RAID 1 to store files.
    """

    def __init__(
        self,
        replication_factor: int,
        send_task_socket: zmq.Socket,
        response_socket: zmq.Socket,
        data_req_socket: zmq.Socket,
    ):
        super().__init__()
        assert replication_factor in [
            2,
            3,
            4,
        ], f"Raid1StorageProvider only supports 2, 3 or 4 replicas, but {replication_factor} were requested"
        assert (
            send_task_socket.type == zmq.PUSH
        ), "send_task_socket must be a ZMQ PUSH socket"
        assert (
            response_socket.type == zmq.PULL
        ), "response_socket must be a ZMQ PULL socket"
        assert (
            data_req_socket.type == zmq.PUB
        ), "data_req_socket must be a ZMQ PUB socket"

        self.replication_factor = replication_factor
        # used to send tasks to the storage nodes (e.g. store data) in the store_file method
        self.send_task_socket = send_task_socket
        # used to receive responses from the storage nodes in the store_file method
        self.response_socket = response_socket
        # used to request data from the storage nodes in the get_file method
        self.data_req_socket = data_req_socket

    def store_file(self, file_data: bytes) -> Tuple[List[str], List[str]]:
        """
        Implements storing a file with RAID 1 using 4 storage nodes.

        :param file_data: A bytearray that holds the file contents
        :return: A list of the random generated chunk names, e.g. (c1,c2), (c3,c4)
        """

        file_size: int = len(file_data)

        # Split the file into self.replicas evenly sized parts
        part_size: int = math.ceil(file_size / self.replication_factor)
        parts: List[bytes] = [
            file_data[i : i + part_size] for i in range(0, file_size, part_size)
        ]
        assert (
            len(parts) == self.replication_factor
        ), f"Expected {self.replication_factor} parts, but got {len(parts)}"

        # Generate random chunk names
        part1_filenames: List[str] = [
            random_string() for _ in range(self.replication_factor)
        ]
        part2_filenames: List[str] = [
            random_string() for _ in range(self.replication_factor)
        ]

        # RAID 1: cut the file in half and store both halves 2x
        file_data_1: bytes = file_data[: math.ceil(file_size / 2.0)]
        file_data_2: bytes = file_data[math.ceil(file_size / 2.0) :]

        # Generate two random chunk names for each half
        file_data_1_names: List[str] = [random_string(8), random_string(8)]
        file_data_2_names: List[str] = [random_string(8), random_string(8)]
        print(f"Filenames for part 1: {file_data_1_names}", file=sys.stderr)
        print(f"Filenames for part 2: {file_data_2_names}", file=sys.stderr)

        # Send 2 'store data' Protobuf requests with the first half and chunk names
        for name in file_data_1_names:
            task = messages_pb2.storedata_request()
            task.filename = name
            self.send_task_socket.send_multipart(
                [task.SerializeToString(), file_data_1]
            )

        # Send 2 'store data' Protobuf requests with the second half and chunk names
        for name in file_data_2_names:
            task = messages_pb2.storedata_request()
            task.filename = name
            self.send_task_socket.send_multipart(
                [task.SerializeToString(), file_data_2]
            )

        # when self.replication_factor == 2, we have 2 parts, and have to send 4 requests
        # when self.replication_factor == 3, we have 3 parts, and have to send 9 requests
        # when self.replication_factor == 4, we have 4 parts, and have to send 16 requests

        num_requests: int = self.replication_factor**2
        print(f"Waiting for {num_requests} responses", file=sys.stderr)

        for _ in range(num_requests):
            resp = self.response_socket.recv_multipart()
            print(f"Received response: {resp}", file=sys.stderr)

        # Wait until we receive 4 responses from the workers
        # for _ in range(4):
        #     # TODO does this block?
        #     resp = self.response_socket.recv_string()
        #     print(f"Received: {resp}", file=sys.stderr)

        # Return the chunk names of each replica
        return file_data_1_names, file_data_2_names

    def get_file(self, part1_filenames: List[str], part2_filenames: List[str]) -> bytes:
        """
        Implements retrieving a file that is stored with RAID 1 using 4 storage nodes.

        :param part1_filenames: List of chunk names that store the first half
        :param part2_filenames: List of chunk names that store the second half
        :param data_req_socket: A ZMQ PUB socket to request chunks from the storage nodes
        :param response_socket: A ZMQ PULL socket where the storage nodes respond.
        :return: The original file contents
        """
        assert (
            len(part1_filenames) == 2
        ), f"part1_filenames must contain 2 filenames, but contains {len(part1_filenames)}"
        assert (
            len(part2_filenames) == 2
        ), f"part2_filenames must contain 2 filenames, but contains {len(part2_filenames)}"
        # Select one chunk of each half
        # part1_filename = part1_filenames[random.randint(0, len(part1_filenames)-1)]
        # part2_filename = part2_filenames[random.randint(0, len(part2_filenames)-1)]
        part1_filename: str = random.choice(part1_filenames)
        part2_filename: str = random.choice(part2_filenames)
        print("Part 1: " + part1_filename)
        print("Part 2: " + part2_filename)

        # Request both chunks in parallel
        task1 = messages_pb2.getdata_request()
        task1.filename = part1_filename
        self.data_req_socket.send(task1.SerializeToString())
        task2 = messages_pb2.getdata_request()
        task2.filename = part2_filename
        self.data_req_socket.send(task2.SerializeToString())

        # Receive both chunks and insert them to
        file_data_parts = [None, None]
        for _ in range(2):
            result = self.response_socket.recv_multipart()
            # First frame: file name (string)
            filename_received = result[0].decode("utf-8")
            # Second frame: data
            chunk_data = result[1]

            print("Received %s" % filename_received)

            if filename_received == part1_filename:
                # The first part was received
                file_data_parts[0] = chunk_data
            else:
                # The second part was received
                file_data_parts[1] = chunk_data

        print("Both chunks received successfully")

        # pp(file_data_parts)

        # Combine the parts and return
        file_data = file_data_parts[0] + file_data_parts[1]
        return file_data
