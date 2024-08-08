"""Top level tet module for storing fixtures."""

import pytest
from gcloud_storage_emulator.server import Server as GCloudStorageMockServer
from google.cloud import storage


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
