from cli import CLI
from dotenv import load_dotenv

"""
main.py

Entry point for the Students & Rooms CLI application.
"""


def main() -> None:
    load_dotenv("credentials.env")
    cli = CLI()
    cli.run()


if __name__ == "__main__":
    main()
