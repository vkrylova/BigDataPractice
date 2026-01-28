from cli import CLI
from dotenv import load_dotenv

"""
app.py

Entry point for the Students & Rooms CLI application.
"""


def main() -> None:
    load_dotenv(".env")
    cli = CLI()
    cli.run()


if __name__ == "__main__":
    main()
