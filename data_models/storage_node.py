import uuid
from dataclasses import dataclass


@dataclass
class StorageNode:
    storage_node_id: str
    address: str
    port_get_data: int
    port_store_data: int
    uid: uuid.UUID
    friendly_name: str
    
    # def __str__(self) -> str:
    #     return f"{self.id} ({self.address}:{self.port})"

    # def __repr__(self) -> str:
    #     return self.__str__()
