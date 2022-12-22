import math
import uuid
from typing import Any, Callable, Dict, List, Optional
import sqlite3

import zmq

from .. import messages_pb2
from ..storage_id import StorageId
from ..storage_node import StorageNode
from ..storage_provider import StorageProvider


class FhdfsStorageProvider(StorageProvider):
    def __init__(
        self,
        replication_factor: int,
        list_of_storage_nodes: List[StorageNode],
        zmq_context: zmq.Context,
        # get_sqlite_connection: Callable[[], sqlite3.Connection],
    ):
        assert replication_factor in [
            2,
            3,
            4,
        ], f"Replication factor must be 2, 3 or 4, not {replication_factor}"
        self.replication_factor = replication_factor
        if len(list_of_storage_nodes) != 4:
            raise ValueError(
                f"Expected 4 storage nodes, got {len(list_of_storage_nodes)}"
            )
        self.list_of_storage_nodes = list_of_storage_nodes
        # Used to create sockets to send data to the storage nodes
        self.zmq_context = zmq_context
        # self.get_sqlite_connection = get_sqlite_connection

    def get_file(self, list_of_storage_ids: List[List[StorageId]]) -> bytes:
        # TODO: use the same function as in raid1.py
        pass

    def store_file(self, file_data: bytes, uid: uuid.UUID) -> List[StorageId]:
        """
        Stores the file data on the storage nodes and returns a list of storage ids.
        NOTE: This method should be called by the storage manager aka. the rest-server.py
        """
        file_size: int = len(file_data)
        # Split the file into self.replicas evenly sized parts
        part_size: int = math.ceil(file_size / self.replication_factor)
        list_of_stripes: List[bytes] = [
            file_data[i : i + part_size] for i in range(0, file_size, part_size)
        ]

        assert (
            len(list_of_stripes) == self.replication_factor
        ), f"Expected {self.replication_factor} stripes, but got {len(list_of_stripes)}"

        # TODO: create self.replication_factor client sockets and connect them to the storage nodes
        request_sockets: List[zmq.Socket] = [
            self.zmq_context.socket(zmq.CLIENT) for _ in range(self.replication_factor)
        ]

        # for socket, storage_node in zip(request_sockets, self.list_of_storage_nodes):
        #     socket.connect(f"tcp://{storage_node.address}:{storage_node.port}")

        # Create a database connection to store to get the storage nodes
        # sqlite_connection = self.get_sqlite_connection()
        # cursor = sqlite_connection.cursor()

        # # select all storage_nodes from the database
        # cursor.execute("SELECT * FROM storage_nodes")
        # rows_of_storage_nodes: List[Any] = cursor.fetchall()

        # num_storage_nodes: int = len(rows_of_storage_nodes)

        # assert num_storage_nodes == 4, f"Expected 4 storage nodes, but got {num_storage_nodes}"

        num_storage_nodes: int = len(self.list_of_storage_nodes)
        node0 = self.list_of_storage_nodes[0]
        node1 = self.list_of_storage_nodes[1]
        node2 = self.list_of_storage_nodes[2]
        node3 = self.list_of_storage_nodes[3]

        match [self.replication_factor, num_storage_nodes]:
            case [2, 4]:
                list_of_nodes_to_forward_to: List[List[Any]] = [
                    [node0, node1],
                    [node2, node3]
                ]
            case [3, 4]:
                list_of_nodes_to_forward_to: List[List[Any]] = [
                    [node0, node1, node2],
                    [node2, node3, node1],
                    [node1, node3, node2]
                ]
            case [4, 4]:
                list_of_nodes_to_forward_to: List[List[Any]] = [
                    [node0, node1, node2, node3],
                    [node2, node3, node1, node0],
                    [node1, node3, node2, node0],
                    [node3, node2, node1, node0]
                ]
            case _:
                raise NotImplementedError(f"Replication factor {self.replication_factor} and number of storage nodes {num_storage_nodes} not implemented")


        # -----------------------------------------------------------------------------------------
        for i, (socket, stripe, nodes_to_forward_to) in enumerate(zip(request_sockets, list_of_stripes, list_of_nodes_to_forward_to)):
            # Create protobuf message
            request = messages_pb2.fhdfs_storedata_request()
            request.stripe_id = i
            request.fragment_id = (
                0  # Each storage node in pipeline will increment this by 1
            )
            request.nodes_to_forward_to = nodes_to_forward_to
            request_serialized: str = request.SerializeToString()
            first_node_to_forward_to = nodes_to_forward_to[0]
            addr: str = first_node_to_forward_to['address']
            port: int = first_node_to_forward_to['port']
            # This call is NON-BLOCKING
            # Send a sequence of buffers as a multipart message.
            socket.connect(f"tcp://{addr}:{port}")
            socket.send_multipart([request_serialized, stripe])

        # -----------------------------------------------------------------------------------------
        storage_ids: List[StorageId] = []
        # await responses from all storage nodes
        for socket in request_sockets:
            response_serialized: bytes = socket.recv()  # This call is BLOCKING
            response: messages_pb2.fhdfs_storedata_response = (
                messages_pb2.fhdfs_storedata_response()
            )
            response.ParseFromString(response_serialized)
            storage_ids.append(
                StorageId(
                    uid,
                    stripe_index=response.stripe_id,
                    replica_index=response.fragment_id,
                )
            )
            # close the socket
            socket.close()

        return storage_ids
