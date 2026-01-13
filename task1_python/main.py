from cli import CLI

"""
main.py

Entry point for the Students & Rooms CLI application.

Responsibilities:
- Initialize and run the command-line interface (CLI)
- Orchestrate the workflow:
    1. Parse CLI arguments (paths to students and rooms files, output format)
    2. Establish a connection to the PostgreSQL database
    3. Load students.json and rooms.json into the database
    4. Create necessary indexes for query optimization
    5. Execute main queries on the database
    6. Export the results in JSON or XML format
- Ensure separation of concerns: the main.py file does not handle file parsing or SQL directly
"""
def main() -> None:
    cli = CLI()
    cli.run()

    if __name__ == "__main__":
        main()
