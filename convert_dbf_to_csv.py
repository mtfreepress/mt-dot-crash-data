import os
import csv
from dbfread import DBF

def convert_dbf_to_csv(dbf_path, csv_path):
    table = DBF(dbf_path, load=True)
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(table.field_names)
        for record in table:
            writer.writerow([record[field] for field in table.field_names])

if __name__ == "__main__":
    # list of years to process
    years = [2019, 2020, 2021, 2022, 2023]

    script_dir = os.path.dirname(os.path.abspath(__file__))

    for year in years:
        dbf_path = os.path.join(script_dir, f"data/Traffic_Yearly_Counts_{year}", f"TYC_{year}.dbf")
        csv_path = os.path.join(script_dir, f"data/Traffic_Yearly_Counts_{year}", f"TYC_{year}.csv")
        if os.path.exists(dbf_path):
            print(f"Converting {dbf_path} to {csv_path} ...")
            convert_dbf_to_csv(dbf_path, csv_path)
            print(f"Converted {dbf_path} to {csv_path}")
        else:
            print(f"File not found: {dbf_path}")