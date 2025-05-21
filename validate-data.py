import pandas as pd
import os

summary_rows = []  # List untuk menyimpan hasil pengecekan ke CSV

with open("data_extract/validation_status.txt", "w") as log_file:
    for csv_file in csv_files:
        data = pd.read_csv(csv_file)

        log_file.write(f"===== Data Quality Pipeline Start for {csv_file} =====\n\n")

        log_file.write("===== Check Data Shape =====\n\n")
        n_column = data.shape[1]
        n_row = data.shape[0]
        log_file.write(f"Data Shape for {csv_file} has {n_row} rows and {n_column} columns\n")

        get_cols = data.columns

        log_file.write("\n\n===== Check Data Types =====\n\n")
        for column in get_cols:
            dtype = data[column].dtypes
            log_file.write(f"Column {column} has data type {dtype}\n")

        log_file.write("\n\n===== Check Missing Values =====\n\n")
        for column in get_cols:
            sum_null = data[column].isnull().sum()
            sum_payment = len(data)
            percen_miss_value = (sum_null / sum_payment) * 100
            log_file.write(f"Columns {column} has percentages missing values {percen_miss_value:.0f}%\n")

            # Tambahkan data ini ke summary CSV
            summary_rows.append({
                "file_name": os.path.basename(csv_file),
                "column_name": column,
                "data_type": str(data[column].dtypes),
                "missing_value_%": round(percen_miss_value, 2),
                "n_rows": n_row,
                "n_columns": n_column
            })

        log_file.write("\n\n===== Data Quality Pipeline End =====\n\n")

# Simpan hasil summary ke CSV
summary_df = pd.DataFrame(summary_rows)
summary_df.to_csv("data_extract/validation_summary.csv", index=False)
