import os
import struct

from cryptography import exceptions
from cryptography.hazmat import backends
from cryptography.hazmat.primitives import ciphers
from cryptography.hazmat.primitives.ciphers import algorithms
from cryptography.hazmat.primitives.ciphers import modes

from tattle import logging

LOG = logging.get_logger(__name__)

VER_SIZE = 1
IV_SIZE = 12
TAG_SIZE = 16


class DecryptError(Exception):
    pass


def _generate_nonce(length=IV_SIZE):
    return os.urandom(length)


def encrypt_data(data, key, version=0):
    nonce = _generate_nonce()

    cipher = ciphers.Cipher(algorithms.AES(key), modes.GCM(nonce), backend=backends.default_backend())

    encryptor = cipher.encryptor()

    cipher_text = encryptor.update(data) + encryptor.finalize()

    tag = encryptor.tag

    return struct.pack('>B', version) + nonce + tag + cipher_text


def decrypt_data(raw, keys=None):
    offset = 0
    version, = struct.unpack('>B', raw[0:VER_SIZE])
    offset += VER_SIZE

    nonce = raw[offset:offset + IV_SIZE]
    offset += IV_SIZE

    tag = raw[offset:offset + TAG_SIZE]
    offset += TAG_SIZE

    cipher_text = raw[offset:]

    for key in keys:
        cipher = ciphers.Cipher(algorithms.AES(key), modes.GCM(nonce, tag), backend=backends.default_backend())

        decryptor = cipher.decryptor()
        
        try:
            plain_text = decryptor.update(cipher_text) + decryptor.finalize()
        except (exceptions.InvalidTag, exceptions.InvalidKey):
            continue

        return plain_text

    raise DecryptError("Failed to decrypt data")
