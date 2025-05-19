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
    factory = GoogleSheetFetcherFactory()
    fetcher = factory.create()
    config_path = CONFIG_DIR / f"{name}.yaml"
    pipeline = ETLPipeline.from_yaml(config_path)
    df = fetcher.fetch(pipeline)
    print(df)


if __name__ == "__main__":
    cli()
