import pytest
from ftplib import FTP


@pytest.mark.usefixtures("mock_ftp")
def test_ftp_mock() -> None:
    """Test FTP client."""
    ftp = FTP()
    ftp.connect("127.0.0.1", 2121)
    ftp.login()
    assert ftp.nlst() == ["harmonised.txt"]
    ftp.quit()
