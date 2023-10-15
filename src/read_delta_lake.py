import polars as pl
from pathlib import Path
from typing import List

# Folder of interest
def dataframe_path(TABLE_PATH, df):
    return str(Path(TABLE_PATH, df))

# Load data
def load_data(path, partition_col, partition: str):
    if partition is None:
        return pl.read_delta(path)
    else:
        return pl.read_delta(path,
                             pyarrow_options={"partitions": [(partition_col, "=", partition)]})

# Main function
def main():
    root = Path(__file__).parent.parent.absolute()
    TABLE_PATH = Path(root, "data", "raw")
    path = dataframe_path(TABLE_PATH, 'schedules')
    df = load_data(path, 'Tm', 'CHC')
    print(f'df shape = {df.shape}')

if __name__=='__main__':
    main()
