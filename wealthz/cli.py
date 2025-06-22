import click as click

from wealthz.constants import CONFIG_DIR
from wealthz.factories import GoogleSheetFetcherFactory
from wealthz.loaders import DuckLakeConnManager, DuckLakeLoader, DuckLakeSchemaSyncer
from wealthz.model import ETLPipeline
from wealthz.settings import DuckLakeSettings
from wealthz.transforms import ColumnTransformEngine


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
    settings = DuckLakeSettings()  # type: ignore[call-arg]
    manager = DuckLakeConnManager(settings)
    conn = manager.provision()
    syncer = DuckLakeSchemaSyncer(conn)
    syncer.sync(pipeline)
    loader = DuckLakeLoader(conn)

    df = fetcher.fetch(pipeline)

    # Apply column-level transforms if configured
    if pipeline.has_transforms:
        transform_engine = ColumnTransformEngine()
        df = transform_engine.apply(df, pipeline.columns)

    loader.load(df, pipeline)
