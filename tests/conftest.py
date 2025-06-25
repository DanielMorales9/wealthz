from pathlib import Path

from dotenv import load_dotenv

# Load test environment early
TEST_DIR = Path(__file__).parent
FIXTURE_DIR = TEST_DIR / "fixtures"
TEST_ENV_PATH = TEST_DIR.parent / ".env.test"
load_dotenv(dotenv_path=TEST_ENV_PATH, override=True)


def load_etl_pipeline(path: Path):
    from wealthz.model import ETLPipeline

    return ETLPipeline.from_yaml(path)


GSHEET_ETL_PIPELINE = load_etl_pipeline(FIXTURE_DIR / "gsheet_etl_pipeline.yaml")
DUCKLAKE_ETL_PIPELINE = load_etl_pipeline(FIXTURE_DIR / "ducklake_etl_pipeline.yaml")
YFINANCE_ETL_PIPELINE = load_etl_pipeline(FIXTURE_DIR / "yfinance_etl_pipeline.yaml")
PEOPLE_ETL_PIPELINE = load_etl_pipeline(FIXTURE_DIR / "people_etl_pipeline.yaml")
