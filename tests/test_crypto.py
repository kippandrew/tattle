import os
import unittest

from tattle import crypto


class CryptoTestCase(unittest.TestCase):
    def test_encrypt_decrypt(self):

        key = os.urandom(16)

        original = b'hello world'

        encrypted = crypto.encrypt_data(original, key)

        # test with valid key
        decrypted = crypto.decrypt_data(encrypted, [key])
        self.assertEqual(decrypted, original)

        # test with invalid key
        with self.assertRaises(crypto.DecryptError):
            crypto.decrypt_data(encrypted, [b'thisisaafakekey!'])

