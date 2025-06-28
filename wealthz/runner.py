from wealthz.factories import FetcherFactory, LoaderFactory, TransformerFactory
from wealthz.loaders import DuckLakeConnManager, DuckLakeSchemaSyncer
from wealthz.logutils import get_logger
from wealthz.model import ETLPipeline
from wealthz.settings import DuckLakeSettings

logger = get_logger(__name__)


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
            syncer = DuckLakeSchemaSyncer(pipeline, conn)
            syncer.sync()

            # Create fetcher and fetch data
            logger.info(f"Fetching data from {pipeline.datasource.type} datasource")
            fetcher_factory = FetcherFactory(pipeline, conn)
            fetcher = fetcher_factory.create()
            df = fetcher.fetch()

            logger.info(f"Fetched {len(df)} rows")

            # Apply transforms if configured
            transformer_factory = TransformerFactory(pipeline)
            transformer = transformer_factory.create()
            df = transformer.transform(df)
            logger.info("Transform application completed")

            # Load data
            loader_factory = LoaderFactory(pipeline, conn)
            loader = loader_factory.create()
            loader.load(df)
            logger.info("Data loading completed")

        except Exception:
            logger.exception("Pipeline execution failed")
            raise
