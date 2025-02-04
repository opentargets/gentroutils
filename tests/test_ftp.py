from ftplib import FTP

import pytest


# @pytest.mark.usefixtures("ftp_client_mock")
@pytest.mark.usefixtures("mock_ftp")
def test_ftp_mock() -> None:
    """Test FTP client."""
    ftp = FTP()
    print("Logging in")
    try:
        ftp.connect("127.0.0.1", 2121, timeout=10)
        print("Connected")
    except Exception as e:
        print(f"Failed to connect: {e}")
    # ftp.login()
    # print("Logged in")
    # print(ftp.nlst())
    # assert ftp.nlst() == ["harmonised.txt"]
    # ftp.quit()
