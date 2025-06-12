from dotenv import load_dotenv

load_dotenv(override=True)

if __name__ == "__main__":
    from wealthz.cli import cli

    cli()
