import json
import pandas as pd
from copy import deepcopy


def read_json(filename: str) -> dict:
    try:
        with open(filename, "r") as f:
            data = json.load(f)  # Use json.load to read the file content as a dictionary
    except Exception as e:
        raise Exception(f"Reading {filename} file encountered an error: {e}")
    return data


def cross_join(left, right):
    new_rows = [] if right else left
    for left_row in left:
        for right_row in right:
            temp_row = deepcopy(left_row)
            for key, value in right_row.items():
                temp_row[key] = value
            new_rows.append(deepcopy(temp_row))
    return new_rows


def flatten_list(data):
    for elem in data:
        if isinstance(elem, list):
            yield from flatten_list(elem)
        else:
            yield elem


def json_to_dataframe(data_in):
    print(type(data_in))
    def flatten_json(data, prev_heading=''):
        if isinstance(data, dict):
            rows = [{}]
            for key, value in data.items():
                rows = cross_join(rows, flatten_json(value, prev_heading + '.' + key))
        elif isinstance(data, list):
            rows = []
            for item in data:
                [rows.append(elem) for elem in flatten_list(flatten_json(item, prev_heading))]
        else:
            rows = [{prev_heading[1:]: data}]
        return rows

    return pd.DataFrame(flatten_json(data_in))



def main(multi_json=False):
    input_file = "in_file.json"
    output_file = "out_file.csv"
    
    # Read the JSON file as a Python dictionary
    data = read_json(filename=input_file)
    
    if multi_json:
        for key, val in data.items():
            split_json = {key: data[key]}
            print(type(val))
            # Generate the DataFrame for the extracted data
            dataframe = json_to_dataframe(data_in=split_json)

            # Convert DataFrame to CSV
            dataframe.to_csv(output_file + '_' + key + '.csv', index=False)
    else:
        dataframe = json_to_dataframe(data_in=data)
        dataframe.to_csv(output_file + '.csv', index=False)

if __name__ == '__main__':
    main()
