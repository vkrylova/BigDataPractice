from airflow.sdk import dag, task, task_group
from airflow.providers.standard.operators.bash import BashOperator
from airflow.datasets import Dataset
from pendulum import datetime
from pathlib import Path

INPUT_DIR = "/opt/airflow/data/input"
FILENAME = "tiktok_google_play_reviews.csv"
OUTPUT_DIR = "/opt/airflow/data/processed"

OUTPUT_FILE_PATH = Path(OUTPUT_DIR) / FILENAME

# Define a dataset for Airflow's data tracking
processed_file = Dataset(str(OUTPUT_FILE_PATH))


@dag(
    dag_id='file_sensor',
    start_date=datetime(2026, 2, 16),
    schedule='*/10 * * * *',
    catchup=False,
    tags=['sensors'],
)
def file_sensor():
    """
    DAG that runs every 10 minutes and processes a CSV file once it is available.

    Workflow:
    1. Wait for the file to appear in the input directory.
    2. Branch depending on whether the file is empty or not.
    3. If the file is empty, log a message.
    4. If the file contains data, process it via a series of tasks:
       - Replace null values
       - Sort by created date
       - Clean text content
       - Move the processed file to the processed directory
    """

    # every 60s, timeout - 9min
    @task.sensor(poke_interval=60, timeout=540, mode="reschedule")
    def wait_for_file() -> bool:
        """
        Sensor task to check for the existence of the input file.

        Pokes every 60 seconds until the file appears or timeout (9 min) is reached.

        :return: True if file exists, False otherwise
        """
        file_path = Path(INPUT_DIR) / FILENAME
        return file_path.exists()

    @task.branch(task_id="check_if_empty")
    def check_if_empty() -> str:
        """
        Branching task to determine if the file is empty.

        Reads the CSV file and checks if it contains any rows.

        :return: Task ID to execute next ("log_empty_file" or "process_data")
        """

        import pandas as pd
        try:
            df = pd.read_csv(Path(INPUT_DIR) / FILENAME)
            return "log_empty_file" if df.empty else "process_data"
        except pd.errors.EmptyDataError:
            return "log_empty_file"

    # Task to log a message if the file is empty
    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command=f'echo "{FILENAME} is empty!"'
    )

    @task_group(group_id="process_data")
    def process_data() -> None:
        """
        Task group to process the CSV file when it is not empty.

        Workflow inside this group:
        1. Replace null values with '-'
        2. Sort data by created date
        3. Clean text content
        4. Move the file to the processed directory
        """

        print(f"Processing {FILENAME}...")
        (replace_all_null() >> sort_data_by_created_date() >>
         clear_data() >> replace_all_null() >> move_file_to_processed_dir())

    @task
    def replace_all_null() -> None:
        """
         Replace all null/missing values in the CSV with a dash '-'.

        Reads the CSV, fills missing values, and writes it back.
        """

        import pandas as pd
        try:
            df = pd.read_csv(Path(INPUT_DIR) / FILENAME)
            df.fillna("-", inplace=True)
            df.to_csv(Path(INPUT_DIR) / FILENAME, index=False)
        except Exception as e:
            print(f"Error processing {FILENAME}: {e}")

    @task
    def sort_data_by_created_date() -> None:
        """
        Sort the CSV data by the 'at' column (created date) in ascending order.

        If the column does not exist, logs a warning and skips sorting.
        """

        import pandas as pd
        try:
            df = pd.read_csv(Path(INPUT_DIR) / FILENAME)
            if "at" not in df.columns:
                print("Warning: 'created_date' column not found. Skipping sort.")
                return

            df['at'] = pd.to_datetime(df['at'], format="%Y-%m-%d %H:%M:%S", errors='coerce')
            df.sort_values(by='at', ascending=True, inplace=True, ignore_index=True)
            df.to_csv(Path(INPUT_DIR) / FILENAME, index=False)
        except Exception as e:
            print(f"Error processing {FILENAME}: {e}")

    @task
    def clear_data() -> None:
        """
        Clean the 'content' column of the CSV by removing special characters.

        Keeps only letters, numbers, whitespace, and basic punctuation.
        """

        import pandas as pd
        import re
        try:
            df = pd.read_csv(Path(INPUT_DIR) / FILENAME)
            df['content'] = (df['content']
                             .map(lambda text: re.sub(r"[^A-Za-z0-9\s.,!?;:'\"-]", "", text)
                                  .strip()))
            df.to_csv(Path(INPUT_DIR) / FILENAME, index=False)
        except Exception as e:
            print(f"Error processing {FILENAME}: {e}")

    @task(outlets=[processed_file])
    def move_file_to_processed_dir() -> None:
        """
        Move the processed CSV file from the input directory to the processed directory.

        This task also defines a Dataset outlet for Airflow's data lineage tracking.
        """

        import shutil
        import pandas as pd

        df = pd.read_csv(Path(INPUT_DIR) / FILENAME)
        try:
            source = Path(INPUT_DIR) / FILENAME
            destination = Path(OUTPUT_DIR) / FILENAME
            shutil.move(source, destination)
            print(f"{FILENAME} moved to processed folder")
        except Exception as e:
            print(f"Error processing {FILENAME}: {e}")

    # Define DAG dependencies
    wait_for_file() >> check_if_empty() >> [process_data(), log_empty_file]


file_sensor()
