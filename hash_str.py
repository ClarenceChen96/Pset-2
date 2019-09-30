from hashlib import sha256

def hash_str(val, salt=''):
    """Converts strings to hash digest

    :param str:
    :param str or bytes salt: string or bytes to add randomness to the hashing, defaults to ''

    :rtype: bytes
    """
    return sha256((salt+val).encode()).digest()
