import logging
import os
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

INPUT_FILE: str = "../data/raw/database.csv"
OUTPUT_DIR: str = "../data/monthly_splits/"


def split_dataset_by_month(input_path: str, output_dir: str) -> None:
    """
    Reads a large CSV dataset in chunks, extracts the month from the departure date,
    and saves the data into separate monthly CSV files.

    Args:
        input_path (str): The path to the raw input CSV file.
        output_dir (str): The directory where the monthly CSV splits will be saved.

    Returns:
        None
    """
    os.makedirs(output_dir, exist_ok=True)

    logger.info(f"Starting to process {input_path}...")

    # Read in chunks to prevent Out-Of-Memory (OOM) errors on 10 million rows
    chunks = pd.read_csv(
        input_path, chunksize=100000, dtype={"departure_id": str, "return_id": str}
    )

    for i, chunk in enumerate(chunks):
        # Dynamically find the date column to handle Kaggle capitalization quirks
        date_col: str = "departure"

        if date_col not in chunk.columns:
            logger.warning(f"No departure column found in chunk {i+1}. Skipping.")
            continue

        # Convert to datetime and drop invalid/missing dates
        chunk[date_col] = pd.to_datetime(chunk[date_col], errors="coerce")
        chunk.dropna(subset=[date_col], inplace=True)

        # Create a temporary column for grouping
        chunk["year_month"] = chunk[date_col].dt.strftime("%Y-%m")

        # Group the chunk by the year_month string
        for period, group in chunk.groupby("year_month"):
            # If pandas gives a tuple (edge case), take the last element
            period_name: str = (
                str(period[-1]) if isinstance(period, tuple) else str(period)
            )

            output_file: str = os.path.join(output_dir, f"{period_name}.csv")

            # Remove the temporary 'year_month' column before saving to keep schema clean
            group_to_save: pd.DataFrame = group.drop(columns=["year_month"])

            # Write header only if the file doesn't exist yet; otherwise, append
            if not os.path.isfile(output_file):
                group_to_save.to_csv(output_file, index=False)
            else:
                group_to_save.to_csv(output_file, mode="a", header=False, index=False)

        logger.info(f"Processed chunk {i+1} ({(i+1)*100000} rows evaluated)")

    logger.info("Data successfully split by month!")

    return None


if __name__ == "__main__":
    split_dataset_by_month(INPUT_FILE, OUTPUT_DIR)
