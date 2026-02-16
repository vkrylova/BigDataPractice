from airflow.sdk import dag, task, task_group
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime
from pathlib import Path

INPUT_DIR = "/opt/airflow/data/input"
FILENAME = "tiktok_google_play_reviews.csv"
OUTPUT_DIR = "/opt/airflow/data/processed"


@dag(
    dag_id='file_sensor',
    start_date=datetime(2026, 2, 16),
    schedule='@daily',
    catchup=False,
    tags=['sensors'],
)
def file_sensor():
    # every 10s
    @task.sensor(poke_interval=10, mode="reschedule")
    def wait_for_file() -> bool:
        file_path = Path(INPUT_DIR) / FILENAME
        return file_path.exists()

    @task.branch(task_id="check_if_empty")
    def check_if_empty():
        import pandas as pd
        try:
            df = pd.read_csv(Path(INPUT_DIR) / FILENAME)
            return "log_empty_file" if df.empty else "process_data"
        except pd.errors.EmptyDataError:
            return "log_empty_file"

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command=f'echo "{FILENAME} is empty!"'
    )

    @task_group(group_id="process_data")
    def process_data() -> None:
        print(f"Processing {FILENAME}...")
        (replace_all_null() >> sort_data_by_created_date() >>
         clear_data() >> replace_all_null() >> move_file_to_processed_dir())

    @task
    def replace_all_null() -> None:
        import pandas as pd
        try:
            df = pd.read_csv(Path(INPUT_DIR) / FILENAME, na_values=["unset"])
            df.fillna("-", inplace=True)
            df.to_csv(Path(INPUT_DIR) / FILENAME, index=False)
        except Exception as e:
            print(f"Error processing {FILENAME}: {e}")

    @task
    def sort_data_by_created_date() -> None:
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

    @task
    def move_file_to_processed_dir() -> None:
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

    wait_for_file() >> check_if_empty() >> [process_data(), log_empty_file]


file_sensor()
