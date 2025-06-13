import os

import click as click

from wealthz.constants import CONFIG_DIR
from wealthz.factories import GoogleSheetFetcherFactory
from wealthz.loaders import DuckLakeConnManager, DuckLakeLoader, StorageSettings
from wealthz.model import ETLPipeline


@click.group()
def cli() -> None:
    pass


@cli.command("run")
@click.argument("name")
def run(name: str) -> None:
    config_path = CONFIG_DIR / f"{name}.yaml"
    pipeline = ETLPipeline.from_yaml(config_path)

    factory = GoogleSheetFetcherFactory(pipeline)
    fetcher = factory.create()
    storage_settings = StorageSettings()  # type: ignore[call-arg]

    PG_CATALOG = {
        "dbname": os.environ["PG_DBNAME"],
        "host": os.environ["PG_HOST"],
        "port": os.environ["PG_PORT"],
        "user": os.environ["PG_USER"],
        "password": os.environ["PG_PASSWORD"],
    }
    DATA_PATH = os.environ["S3_DATA_PATH"]

    manager = DuckLakeConnManager(storage_settings, PG_CATALOG, DATA_PATH)
    conn = manager.provision()
    loader = DuckLakeLoader(conn)

    df = fetcher.fetch()
    loader.load(df, pipeline)
