from pathlib import Path
from typing import Any

import yaml

from wealthz.logutils import get_logger

logger = get_logger(__name__)


def load_yaml(file_path: Path) -> Any:
    with open(file_path) as file:
        obj = yaml.safe_load(file)
    return obj
