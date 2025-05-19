import click as click

from wealthz.constants import CONFIG_DIR
from wealthz.factories import GoogleSheetFetcherFactory
from wealthz.model import ETLPipeline


@click.group()
def cli() -> None:
    pass


@cli.command("extract")
@click.argument("name")
def extract(name: str) -> None:
    config_path = CONFIG_DIR / f"{name}.yaml"
    pipeline = ETLPipeline.from_yaml(config_path)

    file = pipeline.datasource.credentials_file
    factory = GoogleSheetFetcherFactory(file)
    fetcher = factory.create()

    df = fetcher.fetch(pipeline)
    print(df)
