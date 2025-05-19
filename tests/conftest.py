from pathlib import Path

from dotenv import load_dotenv


def pytest_configure():
    test_env_path = Path(__file__).parent.parent / ".env.test"
    load_dotenv(dotenv_path=test_env_path, override=True)
