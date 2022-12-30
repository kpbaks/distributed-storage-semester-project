import fcntl
import logging
import os
import random
import socket
import string
import struct
import subprocess
import time
from typing import Any, Iterable, List, Optional

from constants import *

random.seed(time.time())


def random_string(length: int = 8) -> str:
    """
    Returns a random alphanumeric string of the given length.
    Only lowercase ascii letters and numbers are used.

    :param length: Length of the requested random string
    :return: The random generated string
    """
    return "".join(
        [
            random.SystemRandom().choice(string.ascii_letters + string.digits)
            for n in range(length)
        ]
    )


#


def write_file(data: bytes, filename: Optional[str] = None) -> Optional[str]:
    """
    Write the given data to a local file with the given filename

    :param data: A bytes object that stores the file contents
    :param filename: The file name. If not given, a random string is generated
    :return: The file name of the newly written file, or None if there was an error
    """
    if not filename:
        # Generate random filename
        filename = random_string(8)
        # Add '.bin' extension
        filename += ".bin"

    try:
        # Open filename for writing binary content ('wb')
        # note: when a file is opened using the 'with' statment,
        # it is closed automatically when the scope ends
        with open(f"./{filename}", "wb") as f:
            f.write(data)
    except EnvironmentError as e:
        print("Error writing file: {}".format(e))
        return None

    return filename


#


def is_raspberry_pi() -> bool:
    """
    Returns True if the current platform is a Raspberry Pi, otherwise False.
    """
    return os.uname().nodename == "raspberrypi"


#


def remove_duplicate_from_list(lst: List[Any]) -> List[Any]:
    """
    Returns a new list with duplicate elements removed.
    """
    assert isinstance(lst, list), "The given argument is not a list, but a {}".format(
        type(lst)
    )
    return list(set(lst))


def flatten_list(lst: List[Iterable[Any]]) -> List[Any]:
    """
    Returns a new list with all elements of the given list flattened.
    """
    assert isinstance(lst, list), "The given argument is not a list, but a {}".format(
        type(lst)
    )
    return [item for sublist in lst for item in sublist]


def elements_in_list_are_unique(lst: List[Any]) -> bool:
    """
    Returns True if all elements in the given list are unique, otherwise False.
    """
    assert isinstance(lst, list), f"The given argument is not a list, but a {type(lst)}"
    return len(lst) == len(set(lst))


def create_logger(name: str | None = None, level: int = logging.INFO) -> logging.Logger:
    """
    Returns a logger with the given name and log level.
    """

    format: str = f"[{YELLOW}%(levelname)s{NC}] ({GREEN}%(name)s{NC}) - %(message)s"

    # see if DEBUG is defined as an env var
    if os.environ.get("DEBUG"):
        logging.basicConfig(level=logging.DEBUG, format=format)
    else:
        logging.basicConfig(level=logging.INFO, format=format)

    logger = logging.getLogger(name or __name__)

    logger.info(f"Log level is {get_log_level_name_from_effective_level(logger.getEffectiveLevel())}")
    return logger


def get_log_level_name_from_effective_level(level: int) -> str:
    match level:
        case logging.DEBUG:
            return "DEBUG"
        case logging.INFO:
            return "INFO"
        case logging.WARNING:
            return "WARNING"
        case logging.ERROR:
            return "ERROR"
        case logging.CRITICAL:
            return "CRITICAL"
        case _:
            return "UNKNOWN"


def get_interface_ipaddress(network: str) -> str:
    addr = subprocess.check_output(["ip", "addr", "show", network]).decode("utf-8")
    addr = addr.split("inet ")[1].split("/")[0]
    return addr


def get_random_port_not_in_use() -> int:
    """
    Returns a random port number that is not in use.
    """
    # create a new socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # bind to port 0, which will get a random port number
    s.bind(("", 0))
    # get the port number
    port = s.getsockname()[1]
    # close the socket
    s.close()
    return port