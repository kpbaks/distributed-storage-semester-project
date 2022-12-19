from abc import ABC, abstractmethod
from typing import Any

class StorageProvider(ABC):
    @abstractmethod
    def get_file(self, filename: str) -> bytes:
        pass

    @abstractmethod
    def store_file(self, key, value) -> Any | None:
        pass

