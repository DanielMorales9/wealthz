import click as click

from wealthz.constants import CONFIG_DIR
from wealthz.factories import GoogleSheetFetcherFactory
from wealthz.loaders import DuckLakeConnManager, DuckLakeLoader
from wealthz.model import ETLPipeline
from wealthz.settings import PostgresCatalogSettings, StorageSettings


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
    pg_catalog_settings = PostgresCatalogSettings()  # type: ignore[call-arg]
    manager = DuckLakeConnManager(storage_settings, pg_catalog_settings)
    conn = manager.provision()
    loader = DuckLakeLoader(conn)

    df = fetcher.fetch()
    loader.load(df, pipeline)
