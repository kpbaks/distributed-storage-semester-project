from abc import ABC, abstractmethod

class StorageProvider(ABC):
    @abstractmethod
    def get_file(self, filename: str) -> bytes:
        pass

    @abstractmethod
    def store_file(self, key, value):
        pass

