"""Top level tet module for storing fixtures."""

from dataclasses import dataclass
from pathlib import Path

import pytest
from gcloud_storage_emulator.server import Server as GCloudStorageMockServer
from google.cloud import storage
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import ThreadedFTPServer


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

    # class FTPServerThread(Thread):
    #     def __init__(self, server):
    #         super().__init__()
    #         self.server = server

    #     def start(self):
    #         self.server.serve_forever()

    #     def stop(self):
    #         self.server.close_all()

    # Instantiate a dummy authorizer for managing 'virtual' users

    class MyHandler(FTPHandler):
        def on_connect(self):
            print("%s:%s connected" % (self.remote_ip, self.remote_port))

        def on_disconnect(self):
            # do something when client disconnects
            pass

        def on_login(self, username):
            # do something when user login
            pass

        def on_logout(self, username):
            # do something when user logs out
            pass

        def on_file_sent(self, file):
            # do something when a file has been sent
            pass

        def on_file_received(self, file):
            # do something when a file has been received
            pass

        def on_incomplete_file_sent(self, file):
            # do something when a file is partially sent
            pass

        def on_incomplete_file_received(self, file):
            # remove partially uploaded files
            import os

            os.remove(file)

    def _create_server():
        authorizer = DummyAuthorizer()

        # NOTE: the homedir is the default directory that is mapped to FTP root,
        # the user is able to read, list files and change the directory
        authorizer.add_anonymous("tests/data", perm="elradfmw")

        # Instantiate FTP handler class
        handler = MyHandler
        handler.authorizer = authorizer

        # Define a customized banner (string returned when client connects)
        handler.banner = "ebi ftp server mock ready"

        # Instantiate FTP server class and listen on all interfaces, port 2121
        address = (host, port)
        server = ThreadedFTPServer(address, handler)

        # set a limit for connections
        server.max_cons = 256
        server.max_cons_per_ip = 5
        print(f"Attempting to start FTP server at {host}:{port}")
        server.serve_forever(blocking=False)
        return server

    # start ftp server
    server = _create_server()
    print(f"FTP server started at {host}:{port}")

    yield server
    print(f"Closing FTP server at {host}:{port}")
    # stop ftp server
    server.close_all()


@pytest.fixture(scope="function")
def gwas_catalog_harmonised_list() -> str:
    """Fixture to create a harmonised list file."""
    return "harmonised_list.txt"
