import pandas as pd
import os

INPUT_FILE = "../data/raw/database.csv"
OUTPUT_DIR = "../data/monthly_splits/"
DATE_COLUMN = "departure"

os.makedirs(OUTPUT_DIR, exist_ok=True)
chunks = pd.read_csv(
    INPUT_FILE, chunksize=100000, dtype={"departure_id": str, "return_id": str}
)
for i, chunk in enumerate(chunks):
    chunk[DATE_COLUMN] = pd.to_datetime(chunk[DATE_COLUMN], errors="coerce")
    chunk.dropna(subset=[DATE_COLUMN], inplace=True)
    chunk["year_month"] = chunk[DATE_COLUMN].dt.strftime("%Y-%m")

    # Group the chunk by year_month column and save
    for period, group in chunk.groupby("year_month"):
        # If pandas gives a tuple, take the last element
        if isinstance(period, tuple):
            period_name = str(period[-1])
        else:
            period_name = str(period)

        output_file = os.path.join(OUTPUT_DIR, f"{period_name}.csv")
        # Remove the temporary 'year_month' column before saving
        group_to_save = group.drop(columns=["year_month"])

        if not os.path.isfile(output_file):
            group_to_save.to_csv(output_file, index=False)
        else:
            group_to_save.to_csv(output_file, mode="a", header=False, index=False)

    print(f"Processed chunk {i+1} ({(i+1)*100000} rows evaluated)")
print("Data is split.")
