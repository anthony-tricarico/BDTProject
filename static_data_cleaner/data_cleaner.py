import pandas as pd
import re
import os

def clean_dates():
    input_path = '/data/trentino_trasporti/stop_times.txt'
    output_dir = '/data/trentino_trasporti_cleaned'
    output_path = f'{output_dir}/stop_times_cleaned.txt'

    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(input_path)

    df['arrival_time'] = df['arrival_time'].str.replace(r'\b24:(\d{2})(:\d{2})?\b', r'00:\1\2', regex=True)
    df['departure_time'] = df['departure_time'].str.replace(r'\b24:(\d{2})(:\d{2})?\b', r'00:\1\2', regex=True)

    df.to_csv(output_path, index=False)

if __name__ == "__main__":
    clean_dates()
    print('done')