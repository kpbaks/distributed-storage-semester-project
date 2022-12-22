from dataclasses import dataclass


@dataclass
class StorageNode:
    id: str
    address: str
    port: int
    is_online: bool

    # def __str__(self) -> str:
    #     return f"{self.id} ({self.address}:{self.port})"

    # def __repr__(self) -> str:
    #     return self.__str__()
