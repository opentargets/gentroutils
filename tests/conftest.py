"""Top level tet module for storing fixtures."""

import logging
from collections.abc import Generator
from dataclasses import dataclass
from ftplib import FTP
from pathlib import Path
from threading import Thread

import pytest
from gcloud_storage_emulator.server import Server as GCloudStorageMockServer
from google.cloud import storage
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
from pytest import MonkeyPatch


@pytest.fixture()
def test_data():
    """Fixture to return test data."""
    return str(Path(__file__).parent / "data")


@dataclass
class ConnectionData:
    host: str
    port: int


@pytest.fixture(scope="function")
def google_cloud_storage():
    """Fixture to start the gcloud storage emulator."""
    host = "localhost"
    port = 4443
    in_memory = True
    with pytest.MonkeyPatch.context() as m:
        m.setenv("STORAGE_EMULATOR_HOST", f"http://{host}:{port}")
        emulator = GCloudStorageMockServer(host=host, port=port, in_memory=in_memory)
        emulator.start()
        storage.Client().create_bucket("test")  # Create a test bucket
        yield emulator
        emulator.wipe()
        emulator.stop()


@pytest.mark.usefixtures("google_cloud_storage")
@pytest.fixture(scope="function")
def staging_bucket():
    """Fixture to create a staging bucket."""
    bucket_name = "staging"
    client = storage.Client()
    client.create_bucket(bucket_name)
    return bucket_name


@pytest.mark.usefixtures("google_cloud_storage")
@pytest.fixture(scope="function")
def gwas_catalog_bucket():
    """Fixture to create a gwas catalog bucket."""
    bucket_name = "gwas_catalog"
    client = storage.Client()
    client.create_bucket(bucket_name)
    return bucket_name


@pytest.fixture(scope="function")
def mock_ftp(test_data: str) -> Generator[ConnectionData, None, None]:
    """Fixture to start the ebi ftp server."""
    host = "127.0.0.1"
    port = 2121

    class FTPServerThread(Thread):
        def __init__(self, server):
            super().__init__()
            self.server = server

        def start(self):
            self.server.serve_forever()

        def stop(self):
            self.server.close_all()

    # Instantiate a dummy authorizer for managing 'virtual' users
    def _create_server():
        authorizer = DummyAuthorizer()

        # NOTE: the homedir is the default directory that is mapped to FTP root,
        # the user is able to read, list files and change the directory
        authorizer.add_anonymous(homedir=test_data, perm="lre")

        # Instantiate FTP handler class
        handler = FTPHandler
        handler.authorizer = authorizer

        # Define a customized banner (string returned when client connects)
        handler.banner = "ebi ftp server mock ready"

        # Instantiate FTP server class and listen on all interfaces, port 2121
        address = (host, port)
        server = FTPServer(address, handler)

        # set a limit for connections
        server.max_cons = 256
        server.max_cons_per_ip = 5
        print(f"Attempting to start FTP server at {host}:{port}")
        server.serve_forever(blocking=False)
        return server

    # start ftp server
    server = _create_server()
    print(f"FTP server started at {host}:{port}")

    yield ConnectionData(host=host, port=port)

    # stop ftp server
    server.close_all()


@pytest.fixture(scope="function")
def gwas_catalog_harmonised_list() -> str:
    """Fixture to create a harmonised list file."""
    return "harmonised_list.txt"


@pytest.fixture(scope="function")
def ftp_client_mock(monkeypatch: MonkeyPatch, mock_ftp: ConnectionData) -> None:
    """Fixture to create a ftp client."""

    class MockFTP(FTP):
        def connect(self, host, port, timeout, source_address):
            return super().connect(
                mock_ftp.host, mock_ftp.port, timeout, source_address
            )

    with monkeypatch.context() as m:
        m.setattr("ftplib.FTP", MockFTP)
