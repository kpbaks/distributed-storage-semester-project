from abc import ABC, abstractmethod
from typing import Any

class StorageProvider(ABC):
    # TODO: change arguments to be more specific
    @abstractmethod
    def get_file(self, filename: str) -> bytes:
        pass

    # TODO: change arguments to be more specific
    @abstractmethod
    def store_file(self, key, value) -> Any | None:
        pass

