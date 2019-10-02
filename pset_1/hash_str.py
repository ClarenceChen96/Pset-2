from typing import AnyStr
import os
from hashlib import sha256



def get_csci_salt() -> bytes:
    """Returns the appropriate salt for CSCI E-29"""
    return bytes.fromhex(os.environ['CSCI_SALT'])



def hash_str(some_val: AnyStr, salt: AnyStr = ""):
    """Converts strings to hash digest

    :param some_val: thing to hash

    :param salt: Add randomness to the hashing

    """
    h=sha256()
    if isinstance(salt, bytes):
        h.update(salt)
    else:
        h.update(salt.encode())
    h.update(some_val.encode())
    return h.digest()


def get_user_id(username: str) -> str:
    """hash username with random salt and
    return the first few digits of the hash
    """
    salt = get_csci_salt()
    return hash_str(username.lower(), salt=salt).hex()[:8]

