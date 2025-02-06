"""Top level tet module for storing fixtures."""

import threading
from collections.abc import Generator
from dataclasses import dataclass
from ftplib import FTP

import pytest
from gcloud_storage_emulator.server import Server as GCloudStorageMockServer
from google.cloud import storage
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer


class FTPServerWrapper(threading.Thread):
    """Implementation of the FTP server.

    This server is running on a separate execution thread. The thread runs
    the FTP server in non-blocking mode and loops over the Event that signals
    when to close the server. This is safe to use as a pytest fixture.

    The implementation is based on [pyftpdlib](https://github.com/giampaolo/pyftpdlib/blob/f155ab72b4113db9f2663b42361dfd89db43a78d/pyftpdlib/test/__init__.py#L408C7-L408C24)
    """

    def __init__(self, server: FTPServer):
        self.server = server
        super().__init__()
        self.lock = threading.Lock()
        self._stop_flag = False
        self.started = False
        self._event_stop = threading.Event()

    def run(self):
        """Run the FTP server inside the thread.
        NOTE: Investigate if the sever loop is stopping each time we exit the lock.
        """
        try:
            while not self._stop_flag:
                with self.lock:
                    self.server.serve_forever(timeout=1e-6, blocking=False)

        finally:
            self._event_stop.set()

    def stop(self):
        """Stop the FTP server gracefully with event signal when main thread finishes."""
        self._stop_flag = True  # signal the main loop to exit
        self._event_stop.wait()
        self.server.close_all()
        self.join()


@dataclass
class ConnectionData:
    """Data class representing the FTP Server connection data."""

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
def ftp_server_mock() -> Generator[ConnectionData, None, None]:
    """Fixture to start the ebi ftp server."""
    host = "127.0.0.1"
    port = 2121
    perm = "elr"  # change dir, list, read
    authorizer = DummyAuthorizer()
    authorizer.add_anonymous("tests/data", perm=perm)
    handler = FTPHandler
    handler.authorizer = authorizer

    server = FTPServer((host, port), handler)
    server.max_cons = 5
    server.max_cons_per_ip = 5

    server_thread = FTPServerWrapper(server)
    server_thread.start()

    yield ConnectionData(host, port)
    server_thread.stop()


@pytest.fixture(scope="function")
def ebi_mock_server(monkeypatch: pytest.MonkeyPatch, ftp_server_mock: ConnectionData) -> None:
    """Fixture to mock FTP client to always redirect FTP.connect to mocked server"""

    class MockFTP(FTP):
        """Mock FTP class from ftplib to redirect to FTPServer mock."""

        def connect(self, host, port, timeout, source_address):
            """Connect to the FTP server mock."""
            return super().connect(ftp_server_mock.host, ftp_server_mock.port, timeout, source_address)

    with monkeypatch.context() as m:
        m.setattr("ftplib.FTP", MockFTP)
