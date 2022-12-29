import uuid
from dataclasses import dataclass


@dataclass
class StorageNode:
    storage_node_id: str
    address: str
    port: int
    uid: uuid.UUID
    
    # def __str__(self) -> str:
    #     return f"{self.id} ({self.address}:{self.port})"

    # def __repr__(self) -> str:
    #     return self.__str__()
