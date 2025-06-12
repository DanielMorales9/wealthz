import click as click

from wealthz.constants import CONFIG_DIR
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

    factory = GoogleSheetFetcherFactory(pipeline)
    fetcher = factory.create()
    loader = DuckDBLoader(pipeline)

    df = fetcher.fetch()
    loader.load(df)
