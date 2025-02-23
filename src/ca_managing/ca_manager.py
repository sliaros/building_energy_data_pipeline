import os
import subprocess
import logging
from datetime import datetime, timedelta, timezone
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from src.logging_configuration.logging_config import setup_logging
from typing import Optional

class CaManager:
    def __init__(self,
                 log_file: str = 'C:\\slPrivateData\\00_portfolio\\building_energy_data_pipeline\\logs\\application.log',
                 openssl_path: str = "C:/Program Files/OpenSSL-Win64/bin/openssl.exe",
                 cert_path: str = "server.crt",
                 key_path: str = "server.key",
                 postgresql_conf: str = "C:/Program Files/PostgreSQL/17/data/postgresql.conf",
                 pg_hba_conf: str = "C:/Program Files/PostgreSQL/17/data/pg_hba.conf") -> None:
        """
        Initialize the CaManager with paths for OpenSSL, certificate, key, and PostgreSQL configuration files.

        Args:
            logger (Optional[logging.Logger]): Logger instance for logging. A new logger is created if not provided.
            log_file (str): Path to the log file.
            openssl_path (str): Path to the OpenSSL executable.
            cert_path (str): Default path for the SSL certificate.
            key_path (str): Default path for the SSL key.
            postgresql_conf (str): Path to the PostgreSQL configuration file.
            pg_hba_conf (str): Path to the PostgreSQL HBA configuration file.

        Returns:
            None
        """
        self._openssl_path = openssl_path  # Dependency injection for flexibility
        self._cert_path = cert_path
        self._key_path = key_path
        self._postgresql_conf = postgresql_conf
        self._pg_hba_conf = pg_hba_conf
        self._logger = logging.getLogger(self.__class__.__name__)

        # Setup logging if no handlers are configured
        if not self._logger.hasHandlers():
            setup_logging(log_file=log_file)

    def generate_self_signed_cert(
        self,
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
        common_name: str = "localhost"
    ) -> None:
        """
        Generate a self-signed SSL certificate and private key using OpenSSL.

        Args:
            cert_path (Optional[str], default: None): The path to the certificate file.
                Defaults to the instance variable, `self._cert_path`.
            key_path (Optional[str], default: None): The path to the private key file.
                Defaults to the instance variable, `self._key_path`.
            common_name (str, default: "localhost"): The common name for the certificate.

        Raises:
            FileNotFoundError: If the OpenSSL binary is not found at the specified path.
            subprocess.CalledProcessError: If the OpenSSL command fails.
        """
        cert_path = cert_path or self._cert_path
        key_path = key_path or self._key_path

        try:
            # Generate a self-signed certificate with OpenSSL
            # -new: Generate a new certificate
            # -x509: Generate a self-signed certificate
            # -days 365: Set the certificate to expire in one year
            # -nodes: Do not encrypt the private key
            # -text: Output the certificate in text format
            # -out: The path to the output certificate file
            # -keyout: The path to the output private key file
            # -subj: The subject of the certificate
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

    def generate_cert_with_cryptography(
        self,
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
        common_name: str = "localhost"
    ) -> None:
        """
        Generate a self-signed SSL certificate and private key using the cryptography module.

        Args:
            cert_path (Optional[str], default: None): Path to save the certificate file.
            key_path (Optional[str], default: None): Path to save the private key file.
            common_name (str, default: "localhost"): Common name for the certificate.

        Raises:
            Exception: If there is an error during certificate generation.
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

    def validate_certificate(self, cert_path=None, mode="cryptography", show_details=False):
        """
        Validate an SSL certificate using OpenSSL or the cryptography library.

        Args:
            cert_path (str): Path to the certificate file.
            mode (str): Validation mode, either 'openssl' or 'cryptography'.
            show_details (bool): Whether to print certificate details.

        Raises:
            AssertionError: If mode is not 'openssl' or 'cryptography'.
            subprocess.CalledProcessError: If the OpenSSL command fails.
            Exception: For other validation errors.
        """
        cert_path = cert_path or self._cert_path
        assert mode in ["openssl", "cryptography"], "Invalid mode. Use 'openssl' or 'cryptography'."

        if mode == "openssl":
            self._validate_with_openssl(cert_path, show_details)
        else:
            self._validate_with_cryptography(cert_path, show_details)

    def _validate_with_openssl(self, cert_path, show_details):
        try:
            command = [self._openssl_path, "x509", "-in", cert_path, "-noout"]
            if show_details:
                command.append("-text")
            subprocess.run(command, check=True)
            self._logger.info(f"Certificate {cert_path} is valid (OpenSSL check).")
        except subprocess.CalledProcessError as e:
            self._logger.error(f"Invalid certificate: {e}")
            raise

    def _validate_with_cryptography(self, cert_path, show_details):
        try:
            with open(cert_path, "rb") as cert_file:
                cert = x509.load_pem_x509_certificate(cert_file.read())
                if show_details:
                    self._log_certificate_details(cert, cert_path)
            self._logger.info(f"Certificate {cert_path} is valid (cryptography check).")
        except Exception as e:
            self._logger.error(f"Invalid certificate using cryptography check: {e}")
            raise

    def _log_certificate_details(self, cert, cert_path):
        """
        Prints the details of an SSL certificate for debugging purposes.

        Args:
            cert (cryptography.x509.Certificate): The SSL certificate object.
            cert_path (str): The path to the certificate file.
        """
        # Print the certificate details
        self._logger.info(f"Certificate {cert_path} details:")
        self._logger.info(f"  Subject: {cert.subject}")
        # The subject is the entity to which the certificate is issued.
        # It is usually a tuple of values, such as the organization name,
        # the country name, etc.
        self._logger.info(f"  Issuer: {cert.issuer}")
        # The issuer is the entity that issued the certificate. It is usually
        # the same as the subject, but it can be different.
        self._logger.info(f"  Serial Number: {cert.serial_number}")
        # The serial number is a unique identifier for the certificate.
        self._logger.info(f"  Valid From: {cert.not_valid_before}")
        # The not valid before date is the start of the period during which
        # the certificate is valid.
        self._logger.info(f"  Valid To: {cert.not_valid_after}")
        # The not valid after date is the end of the period during which
        # the certificate is valid.

    def configure_postgresql_ssl(self, postgresql_conf = None, pg_hba_conf = None,
                                 cert_path = None, key_path = None, enable_ssl=True):
        """
        Configure PostgreSQL to use SSL.

        This function will append the necessary configuration to the
        postgresql.conf and pg_hba.conf files. If you want to disable SSL,
        you can pass enable_ssl=False.

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

        # Open the postgresql.conf file and read its content
        with open(postgresql_conf, "r+") as f:
            content = f.read()

            # If we want to enable SSL, make sure the configuration is correct
            if enable_ssl:
                # Check if the configuration is already correct
                if "ssl = off" in content:
                    # If not, change ssl = off to ssl = on and add the certificate and key paths
                    content = content.replace("ssl = off", "ssl = on")
                    content += f"\nssl_cert_file = '{cert_path}'\n"
                    content += f"ssl_key_file = '{key_path}'\n"
            else:
                # If we want to disable SSL, make sure the configuration is correct
                if "ssl = on" in content:
                    # If not, change ssl = on to ssl = off and remove the certificate and key paths
                    content = content.replace("ssl = on", "ssl = off")
                    content = content.replace(f"\nssl_cert_file = '{cert_path}'\n", "")
                    content = content.replace(f"\nssl_key_file = '{key_path}'\n", "")

            # Write the updated configuration to the file
            f.seek(0)
            f.write(content)
            f.truncate()

        # Open the pg_hba.conf file and read its content
        with open(pg_hba_conf, "r+") as f:
            content = f.read()

            # If we want to enable SSL, make sure the configuration is correct
            if enable_ssl:
                # Check if the configuration is already correct
                if "host all all 0.0.0.0/0 md5" in content:
                    # If not, change host all all 0.0.0.0/0 md5 to hostssl all all 0.0.0.0/0 md5
                    content = content.replace("host all all 0.0.0.0/0 md5", "hostssl all all 0.0.0.0/0 md5")
            else:
                # If we want to disable SSL, make sure the configuration is correct
                if "hostssl all all 0.0.0.0/0 md5" in content:
                    # If not, change hostssl all all 0.0.0.0/0 md5 to host all all 0.0.0.0/0 md5
                    content = content.replace("hostssl all all 0.0.0.0/0 md5", "host all all 0.0.0.0/0 md5")

            # Write the updated configuration to the file
            f.seek(0)
            f.write(content)
            f.truncate()

        # Log the result
        if enable_ssl:
            self._logger.info("PostgreSQL SSL configuration enabled successfully.")
        else:
            self._logger.info("PostgreSQL SSL configuration disabled successfully.")


if __name__ == "__main__":
    # Example usage
    ssl_manager = CaManager()
    ssl_manager.generate_self_signed_cert()
    ssl_manager.validate_certificate()
    ssl_manager.configure_postgresql_ssl(enable_ssl=True)