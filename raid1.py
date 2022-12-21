import math
import os
import sys
import random
import sys
from pprint import pp
from typing import List, Tuple
import logging
import itertools as it



import zmq

import messages_pb2
from storage_provider import StorageProvider
from utils import random_string, remove_duplicate_from_list

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

        self.logger = logging.getLogger("raid1_storage_provider")
        
        if os.environ.get("DEBUG"):
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)



    # def store_file(self, file_data: bytes) -> Tuple[List[str], List[str]]:
    def store_file(self, file_data: bytes) -> List[List[str]]:
        """
        Implements storing a file with RAID 1 using 4 storage nodes.

        :param file_data: A bytearray that holds the file contents
        :return: A list of the random generated stripe names, e.g. (c1,c2), (c3,c4)
        """

        file_size: int = len(file_data)

        # Split the file into self.replicas evenly sized parts
        part_size: int = math.ceil(file_size / self.replication_factor)
        list_of_stripe_data: List[bytes] = [
            file_data[i : i + part_size] for i in range(0, file_size, part_size)
        ]

        list_of_stripe_names: List[List[str]] = [
            [random_string(16) for _ in range(self.replication_factor)] # TODO: Report - mention that we're not checking for duplicates in the data directory
            for _ in range(self.replication_factor)
        ]
        
        self.logger.debug(f"list_of_stripe_names: {list_of_stripe_names}")

        for stripe_names in list_of_stripe_names:
            assert len(stripe_names) == len(list_of_stripe_data), f"stripe_names: {stripe_names} must have length {len(list_of_stripe_data)}"
            for stripe, name in zip(list_of_stripe_data, stripe_names):
                task = messages_pb2.storedata_request()
                task.filename = name
                self.send_task_socket.send_multipart(
                    [task.SerializeToString(), stripe]
                )

        num_requests: int = self.replication_factor**2
        print(f"Waiting for {num_requests} responses", file=sys.stderr)

        # Synchonously wait for the responses
        for i in range(num_requests):
            resp = self.response_socket.recv_multipart()
            print(f"Received response [{i+1}/{num_requests}]: {resp}", file=sys.stderr)

        pp(list_of_stripe_names)
        return list_of_stripe_names


    def get_file(self, list_of_stripe_uids: List[List[str]]) -> bytes:
        """
        Implements retrieving a file that is stored with RAID 1 using 4 storage nodes.

        :param part1_filenames: List of stripe names that store the first half
        :param part2_filenames: List of stripe names that store the second half
        :param data_req_socket: A ZMQ PUB socket to request stripes from the storage nodes
        :param response_socket: A ZMQ PULL socket where the storage nodes respond.
        :return: The original file contents
        """

        replication_factor = len(list_of_stripe_uids)
        assert replication_factor in [2,3,4], f"Raid1StorageProvider only supports 2, 3 or 4 replicas, but {replication_factor} were requested"
        self.logger.info(f"get_file: list_of_stripe_uids: {list_of_stripe_uids}")

        # Select a random stripe from each stripe set
        stripe_uids_to_request = [random.choice(stripe_uids) for stripe_uids in list_of_stripe_uids]
        self.logger.debug(f"stripe_uids_to_request: {stripe_uids_to_request}")
        
        # Request the stripes in parallel
        for stripe_uid in stripe_uids_to_request:
            task = messages_pb2.getdata_request()
            task.filename = stripe_uid
            self.data_req_socket.send(task.SerializeToString())

        # Receive all stripes and collect them 
        file_data_parts = [None] * replication_factor
        for _ in range(replication_factor):
            result = self.response_socket.recv_multipart()
            # First frame: file name (string)
            filename_received = result[0].decode("utf-8")
            assert filename_received in stripe_uids_to_request, f"Received unexpected filename {filename_received}"
            # Second frame: data
            stripe_data = result[1]

            self.logger.debug(f"Received {filename_received}")
            idx: int = stripe_uids_to_request.index(filename_received)
            self.logger.debug(f"Inserting at index {idx}")
            assert stripe_uids_to_request[idx] == filename_received, f"stripe_uids_to_request[{idx}] != filename_received"
            file_data_parts[idx] = stripe_data

        
        assert None not in file_data_parts, "Not all stripes received successfully"
        self.logger.info("All stripes received successfully")
        
        # Concatenates the stripes in one bytes object
        file_data = b"".join(file_data_parts)

        return file_data
