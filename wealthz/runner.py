from wealthz.factories import FetcherFactory, LoaderFactory, TransformerFactory
from wealthz.loaders import DuckLakeConnManager, DuckLakeSchemaSyncer
from wealthz.logutils import get_logger
from wealthz.model import ETLPipeline
from wealthz.settings import DuckLakeSettings

logger = get_logger(__name__)


class PipelineRunner:
    """Service class for executing ETL pipelines."""

    def __init__(self, pipeline: ETLPipeline) -> None:
        self._pipeline = pipeline
        self.settings = DuckLakeSettings()  # type: ignore[call-arg]
        self.conn_manager = DuckLakeConnManager(self.settings)

        conn = self.conn_manager.provision()
        self._syncer = DuckLakeSchemaSyncer(pipeline, conn)

        fetcher_factory = FetcherFactory(pipeline, conn)
        self._fetcher = fetcher_factory.create()

        transformer_factory = TransformerFactory(pipeline)
        self._transformer = transformer_factory.create()

        loader_factory = LoaderFactory(pipeline, conn)
        self._loader = loader_factory.create()

    def run(self) -> None:
        """Execute the ETL pipeline."""
        try:
            logger.info(f"Starting pipeline execution: {self._pipeline.name}")

            logger.info("Syncing database schema")
            self._syncer.sync()

            # Create fetcher and fetch data
            logger.info(f"Fetching data from {self._pipeline.datasource.type} datasource")
            df = self._fetcher.fetch()
            logger.info(f"Fetched {len(df)} rows")

            # Apply transforms if configured
            df = self._transformer.transform(df)
            logger.info("Transform application completed")

            # Load data
            logger.info(f"Loading data into {self._pipeline.destination.type} destination")
            self._loader.load(df)
            logger.info("Data loading completed")

        except Exception:
            logger.exception("Pipeline execution failed")
            raise
