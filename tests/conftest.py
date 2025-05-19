import os


def pytest_configure():
    os.environ["GOOGLE_CREDENTIALS_FILENAME"] = "mock_creds.json"
