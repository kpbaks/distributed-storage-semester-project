import math
import os
import sys
import random
import sys
from pprint import pp
from typing import List, Tuple
import logging
import itertools as it
import uuid



import zmq

import messages_pb2
from storage_provider import StorageProvider
from utils import random_string, remove_duplicate_from_list
from storage_id import StorageId

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
    def store_file(self, file_data: bytes, uid: uuid.UUID) -> List[List[StorageId]]:
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

        # storage_ids = []
        # for stripe_idx in range(self.replication_factor):
        #     for replica_idx in range(self.replication_factor):
        #         storage_ids.append(StorageId(uid, stripe_index=stripe_idx, replica_index=replica_idx))

        # [ [ (0, 0), (0, 1) ], [ (1,0), (1, 1) ]]



        list_of_storage_ids = [
            [
                StorageId(uid, stripe_index=stripe_index, replica_index=replica_index)
                for stripe_index in range(self.replication_factor)
            ] for replica_index in range(self.replication_factor)
        ]

        self.logger.debug(f"list_of_storage_ids: {list_of_storage_ids}")

        # for storage_id in storage_ids:
        #     self.logger.debug(f"storage_id: {storage_id}")


        for storage_ids in list_of_storage_ids:
            assert len(storage_ids) == len(list_of_stripe_data), f"storage_ids: {storage_ids} must have length {len(list_of_stripe_data)}"
            for stripe, storage_id in zip(list_of_stripe_data, storage_ids):
                task = messages_pb2.storedata_request()
                task.filename = str(storage_id)
                self.send_task_socket.send_multipart(
                    [task.SerializeToString(), stripe]
                )

        num_requests: int = self.replication_factor**2
        self.logger.info(f"Waiting for {num_requests} responses")

        # Synchonously wait for the responses
        for i in range(num_requests):
            resp = self.response_socket.recv_multipart()
            print(f"Received response [{i+1}/{num_requests}]: {resp}", file=sys.stderr)

        return list_of_storage_ids


    def get_file(self, list_of_storage_ids: List[List[StorageId]]) -> bytes:
        """
        Implements retrieving a file that is stored with RAID 1 using 4 storage nodes.

        :return: The original file contents
        """

        replication_factor = len(list_of_storage_ids)
        assert replication_factor in [2,3,4], f"Raid1StorageProvider only supports 2, 3 or 4 replicas, but {replication_factor} were requested"
        self.logger.debug(f"get_file: list_of_stripe_uids: {list_of_storage_ids}")


        # Select a random stripe from each stripe set
        storage_ids_to_request: List[StorageId] = [random.choice(storage_ids) for storage_ids in list_of_storage_ids]
        self.logger.debug(f"storage_ids_to_request: {storage_ids_to_request}")
        
        # Request the stripes in parallel
        for storage_id in storage_ids_to_request:
            task = messages_pb2.getdata_request()
            task.filename = str(storage_id)
            self.data_req_socket.send(task.SerializeToString())

        # Receive all stripes and collect them 
        # file_data_parts = [None] * replication_factor
        file_data_parts = []
        for _ in range(replication_factor):

            result = self.response_socket.recv_multipart()
            # First frame: file name (string)
            filename_received = result[0].decode("utf-8")
            storage_id_received = StorageId.from_string(filename_received)
            self.logger.debug(f"RECEIVED STRIPE {storage_id_received}")
            # Second frame: data
            stripe_data = result[1]

            file_data_parts.append((storage_id_received, stripe_data))
            # file_data_parts[storage_id_received.stripe_index] = stripe_data

        #breakpoint()
        file_data_parts.sort(key=lambda x: x[0].stripe_index)


        
        assert None not in file_data_parts, "Not all stripes received successfully"
        self.logger.info("All stripes received successfully")
        
        # Concatenates the stripes in one bytes object
        file_data = b"".join([stripe_data for _, stripe_data in file_data_parts])

        return file_data
