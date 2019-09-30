from typing import AnyStr
import os
from hashlib import sha256



def get_csci_salt() -> bytes:
    """Returns the appropriate salt for CSCI E-29"""
    return bytes.fromhex(os.environ['CSCI_SALT'])

    # Hint: use os.environment and bytes.fromhex


def hash_str(some_val: AnyStr, salt: AnyStr = ""):
    """Converts strings to hash digest

    :param some_val: thing to hash

    :param salt: Add randomness to the hashing

    """
    return sha256((salt+some_val).encode()).digest()


def get_user_id(username: str) -> str:
    salt = get_csci_salt()
    return hash_str(username.lower(), salt=salt).hex()[:8]

