import os
import subprocess
import logging
from datetime import datetime, timedelta, timezone
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from logs.logging_config import setup_logging

class CaManager:
    def __init__(self, logger = None,
                 log_file = 'C:\\slPrivateData\\00_portfolio\\building_energy_data_pipeline\\logs\\application.log',
                 openssl_path="C:/Program Files/OpenSSL-Win64/bin/openssl.exe",
                 cert_path: str = "server.crt",
                 key_path: str = "server.key",
                 postgresql_conf: str = "C:/Program Files/PostgreSQL/17/data/postgresql.conf",
                 pg_hba_conf: str = "C:/Program Files/PostgreSQL/17/data/pg_hba.conf"):

        self._openssl_path = openssl_path  # Dependency injection for flexibility
        self._cert_path = cert_path
        self._key_path = key_path
        self._postgresql_conf = postgresql_conf
        self._pg_hba_conf = pg_hba_conf
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        if not self._logger.hasHandlers():
            setup_logging(
                log_file=log_file)

    def generate_self_signed_cert(self, cert_path = None, key_path = None, common_name="localhost"):
        """
        Generate a self-signed SSL certificate and private key using OpenSSL.
        """
        cert_path = cert_path or self._cert_path
        key_path = key_path or self._key_path

        try:
            subprocess.run([
                self._openssl_path, "req", "-new", "-x509", "-days", "365", "-nodes", "-text",
                "-out", cert_path, "-keyout", key_path, "-subj", f"/CN={common_name}"
            ], check=True)
            os.chmod(key_path, 0o600)  # Secure the private key
            self._logger.info(f"Self-signed certificate generated: {cert_path}, {key_path}")
        except FileNotFoundError:
            self._logger.error(f"OpenSSL binary not found at {self._openssl_path}")
        except subprocess.CalledProcessError as e:
            self._logger.error(f"Error generating SSL certificate: {e}")
            raise

    def generate_cert_with_cryptography(self, cert_path = None, key_path = None, common_name="localhost"):
        """
        Generate a self-signed SSL certificate and private key using the cryptography module.
        """
        cert_path = cert_path or self._cert_path
        key_path = key_path or self._key_path

        try:
            key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048
            )
            subject = issuer = x509.Name([
                x509.NameAttribute(NameOID.COMMON_NAME, common_name)
            ])
            cert = (
                x509.CertificateBuilder()
                .subject_name(subject)
                .issuer_name(issuer)
                .public_key(key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(datetime.now(timezone.utc))
                .not_valid_after(datetime.now(timezone.utc) + timedelta(days=365))
                .add_extension(
                    x509.BasicConstraints(ca=True, path_length=None), critical=True
                )
                .sign(key, hashes.SHA256())
            )

            with open(cert_path, "wb") as cert_file:
                cert_file.write(cert.public_bytes(serialization.Encoding.PEM))
            with open(key_path, "wb") as key_file:
                key_file.write(
                    key.private_bytes(
                        encoding=serialization.Encoding.PEM,
                        format=serialization.PrivateFormat.TraditionalOpenSSL,
                        encryption_algorithm=serialization.NoEncryption(),
                    )
                )
            os.chmod(key_path, 0o600)  # Secure the private key
            self._logger.info(f"Self-signed certificate generated using cryptography: {cert_path}, {key_path}")
        except Exception as e:
            self._logger.error(f"Error generating certificate with cryptography: {e}")
            raise

    def validate_certificate(self, cert_path = None, mode="cryptography", print_details=False):
        """
        Validate an SSL certificate using OpenSSL.
        """
        cert_path = cert_path or self._cert_path

        assert mode in ["openssl", "cryptography"], "Invalid mode. Use 'openssl' or 'cryptography'."

        if mode == "openssl":
            try:
                _command = [self._openssl_path, "x509", "-in", cert_path, "-noout"]
                if print_details:
                    _command.append("-text")
                subprocess.run(_command, check=True)
                self._logger.info(f"Certificate {cert_path} is valid (OpenSSL check).")
            except subprocess.CalledProcessError as e:
                self._logger.error(f"Invalid certificate: {e}")
                raise

        else:
            try:
                with open(cert_path, "rb") as cert_file:
                    _cert = x509.load_pem_x509_certificate(cert_file.read())
                    if print_details:
                        self._logger.info(f"Certificate {cert_path} details:")
                        self._logger.info(f"  Subject: {_cert.subject}")
                        self._logger.info(f"  Issuer: {_cert.issuer}")
                        self._logger.info(f"  Serial Number: {_cert.serial_number}")
                        self._logger.info(f"  Not Before: {_cert.not_valid_before}")
                        self._logger.info(f"  Not After: {_cert.not_valid_after}")
                self._logger.info(f"Certificate {cert_path} is valid (cryptography check).")
            except Exception as e:
                self._logger.error(f"Invalid certificate using cryptography check: {e}")
                raise

    def configure_postgresql_ssl(self, postgresql_conf = None, pg_hba_conf = None,
                                 cert_path = None, key_path = None, enable_ssl=True):
        """
        Configure PostgreSQL to use SSL.

        Args:
            postgresql_conf (str): Path to the postgresql.conf file.
            pg_hba_conf (str): Path to the pg_hba.conf file.
            cert_path (str): Path to the SSL certificate file.
            key_path (str): Path to the SSL key file.
            enable_ssl (bool): Whether to enable or disable SSL. Defaults to True.
        """
        cert_path = cert_path or self._cert_path
        key_path = key_path or self._key_path
        postgresql_conf = postgresql_conf or self._postgresql_conf
        pg_hba_conf = pg_hba_conf or self._pg_hba_conf

        try:
            with open(postgresql_conf, "a") as f:
                if enable_ssl:
                    f.write("\n# SSL Configuration\n")
                    f.write("ssl = on\n")
                    f.write(f"ssl_cert_file = '{cert_path}'\n")
                    f.write(f"ssl_key_file = '{key_path}'\n")
                else:
                    f.write("\n# Disable SSL\n")
                    f.write("ssl = off\n")

            with open(pg_hba_conf, "a") as f:
                if enable_ssl:
                    f.write("\n# Require SSL for remote connections\n")
                    f.write("hostssl all all 0.0.0.0/0 md5\n")
                else:
                    f.write("\n# Disable SSL for remote connections\n")
                    f.write("host all all 0.0.0.0/0 md5\n")

            if enable_ssl:
                self._logger.info("PostgreSQL SSL configuration enabled successfully.")
            else:
                self._logger.info("PostgreSQL SSL configuration disabled successfully.")
        except Exception as e:
            self._logger.error(f"Error configuring PostgreSQL SSL: {e}")
            raise


if __name__ == "__main__":
    # Example usage
    ssl_manager = CaManager()
    ssl_manager.generate_self_signed_cert()
    ssl_manager.validate_certificate()
    ssl_manager.configure_postgresql_ssl(enable_ssl=True)