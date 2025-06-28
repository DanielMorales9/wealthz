import sys

import click
from pydantic import ValidationError

from wealthz.constants import CONFIG_DIR
from wealthz.logutils import get_logger
from wealthz.model import ETLPipeline
from wealthz.runner import PipelineRunner

logger = get_logger(__name__)


@click.group()
def cli() -> None:
    """Wealthz ETL Pipeline CLI."""
    pass


@cli.command("run")
@click.argument("name")
def run(name: str) -> None:
    """Run an ETL pipeline by name.

    NAME: The name of the pipeline configuration file (without .yaml extension)
    """
    try:
        # Validate config file exists
        config_path = CONFIG_DIR / f"{name}.yaml"
        if not config_path.exists():
            click.echo(f"Error: Configuration file not found: {config_path}", err=True)
            click.echo(f"Available configs in {CONFIG_DIR}:", err=True)
            for yaml_file in CONFIG_DIR.glob("*.yaml"):
                click.echo(f"  - {yaml_file.stem}", err=True)
            sys.exit(1)

        # Load and validate pipeline configuration
        try:
            pipeline = ETLPipeline.from_yaml(config_path)
        except ValidationError as e:
            click.echo(f"Error: Invalid pipeline configuration: {e}", err=True)
            sys.exit(1)
        except Exception as e:
            click.echo(f"Error: Failed to load configuration: {e}", err=True)
            sys.exit(1)

        # Execute pipeline
        runner = PipelineRunner(pipeline)
        runner.run()

        click.echo("Pipeline executed successfully")

    except KeyboardInterrupt:
        click.echo("\nPipeline execution interrupted by user", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Pipeline execution failed: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
