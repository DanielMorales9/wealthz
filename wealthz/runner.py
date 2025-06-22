from wealthz.cli import logger
from wealthz.factories import FetcherFactory
from wealthz.loaders import DuckLakeConnManager, DuckLakeLoader, DuckLakeSchemaSyncer
from wealthz.model import ETLPipeline
from wealthz.settings import DuckLakeSettings
from wealthz.transforms import ColumnTransformEngine


class PipelineRunner:
    """Service class for executing ETL pipelines."""

    def __init__(self) -> None:
        self.settings = DuckLakeSettings()  # type: ignore[call-arg]
        self.conn_manager = DuckLakeConnManager(self.settings)

    def run(self, pipeline: ETLPipeline) -> None:
        """Execute the ETL pipeline."""
        try:
            logger.info(f"Starting pipeline execution: {pipeline.name}")

            # Setup connection and sync schema
            conn = self.conn_manager.provision()

            logger.info("Syncing database schema")
            syncer = DuckLakeSchemaSyncer(conn)
            syncer.sync(pipeline)

            # Create fetcher and fetch data
            logger.info(f"Fetching data from {pipeline.datasource.type} datasource")
            fetcher = FetcherFactory.create_fetcher(pipeline, conn)
            df = fetcher.fetch(pipeline)

            logger.info(f"Fetched {len(df)} rows")

            # Apply transforms if configured
            if pipeline.has_transforms:
                logger.info("Applying column transforms")
                transform_engine = ColumnTransformEngine()
                df = transform_engine.apply(df, pipeline.columns)
                logger.info("Transform application completed")

            # Load data
            logger.info(f"Loading {len(df)} rows to {pipeline.name}")
            loader = DuckLakeLoader(conn)
            loader.load(df, pipeline)
            logger.info("Data loading completed")

        except Exception:
            logger.exception("Pipeline execution failed")
            raise
