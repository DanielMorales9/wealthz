import os
from pathlib import Path

SECRETS_PATH = Path(__file__).parent.parent / "secrets"
GOOGLE_CREDENTIALS_FILENAME = os.environ["GOOGLE_CREDENTIALS_FILENAME"]
