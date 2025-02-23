import unittest
import tempfile
import os
from unittest.mock import patch, MagicMock
from src.ca_managing.ca_manager import CaManager
import subprocess

class TestCaManagerGenerateSelfSignedCert(unittest.TestCase):
    def setUp(self):
        self.ca_manager = CaManager()

    def test_success(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cert_path = os.path.join(temp_dir, "cert.crt")
            key_path = os.path.join(temp_dir, "key.key")
            self.ca_manager.generate_self_signed_cert(cert_path, key_path)
            self.assertTrue(os.path.exists(cert_path))
            self.assertTrue(os.path.exists(key_path))

    @patch("subprocess.run")
    def test_missing_openssl_binary(self, mock_subprocess):
        mock_subprocess.side_effect = FileNotFoundError("OpenSSL not found")
        with self.assertRaises(FileNotFoundError):
            self.ca_manager.generate_self_signed_cert()

    @patch("subprocess.run")
    def test_invalid_openssl_command(self, mock_subprocess):
        mock_subprocess.side_effect = subprocess.CalledProcessError(1, "Invalid command")
        with self.assertRaises(subprocess.CalledProcessError):
            self.ca_manager.generate_self_signed_cert()

    def test_default_paths(self):
        self.ca_manager.generate_self_signed_cert()
        self.assertTrue(os.path.exists(self.ca_manager._cert_path))
        self.assertTrue(os.path.exists(self.ca_manager._key_path))

    def test_custom_paths(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cert_path = os.path.join(temp_dir, "cert.crt")
            key_path = os.path.join(temp_dir, "key.key")
            self.ca_manager.generate_self_signed_cert(cert_path, key_path)
            self.assertTrue(os.path.exists(cert_path))
            self.assertTrue(os.path.exists(key_path))

    def test_custom_common_name(self):
        common_name = "example.com"
        with tempfile.TemporaryDirectory() as temp_dir:
            cert_path = os.path.join(temp_dir, "cert.crt")
            key_path = os.path.join(temp_dir, "key.key")
            self.ca_manager.generate_self_signed_cert(cert_path, key_path, common_name)
            self.assertTrue(os.path.exists(cert_path))
            self.assertTrue(os.path.exists(key_path))

if __name__ == "__main__":
    unittest.main()