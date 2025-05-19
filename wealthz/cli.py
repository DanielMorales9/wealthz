import click as click

from wealthz.factories import GoogleSheetFetcherFactory


@click.group()
def cli() -> None:
    pass


@cli.command("extract")
@click.argument("sheet_id")
@click.argument("sheet_range")
def extract(sheet_id: str, sheet_range: str) -> None:
    factory = GoogleSheetFetcherFactory(sheet_id, sheet_range)
    fetcher = factory.create()
    df = fetcher.fetch()
    print(df)


if __name__ == "__main__":
    cli()
