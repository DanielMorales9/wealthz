import click as click

from wealthz.constants import CONFIG_DIR, DUCKDB_LOCAL_DATA_PATH, DUCKDB_LOCAL_META_PATH
from wealthz.factories import GoogleSheetFetcherFactory
from wealthz.loaders import DuckDBLoader
from wealthz.model import ETLPipeline


@click.group()
def cli() -> None:
    pass


@cli.command("run")
@click.argument("name")
def run(name: str) -> None:
    config_path = CONFIG_DIR / f"{name}.yaml"
    pipeline = ETLPipeline.from_yaml(config_path)

    credentials_file = pipeline.datasource.credentials_file
    factory = GoogleSheetFetcherFactory(credentials_file)
    fetcher = factory.create()
    loader = DuckDBLoader(DUCKDB_LOCAL_META_PATH, DUCKDB_LOCAL_DATA_PATH)

    df = fetcher.fetch(pipeline)
    loader.load(df, pipeline)
