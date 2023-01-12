import numpy as np
import pandas as pd


# Return number of '|' characters in string
def num_bars(_string):
    return np.count_nonzero([c == '|' for c in list(_string)])


# Parse string into array of column values
def read_row(row, fields):
    remaining_row = row
    dat = []
    for field in fields:
        item = None
        try:
            if remaining_row[0] == ',':
                item = None
                remaining_row = remaining_row[1:]
            elif field['type'] == 'char' or field['type'] == 'varchar':
                end_char = remaining_row[1:].index('|') + 1
                item = remaining_row[1:end_char]
                remaining_row = remaining_row[(end_char+2):]
            else:
                end_char = remaining_row.index(',')
                item = remaining_row[:end_char]
                remaining_row = remaining_row[(end_char+1):]
            dat.append(item)
        except Exception as ex:
            print(ex)
            raise ValueError('Failed to parse row: %s' % row)
    
    return dat


# Read in text file and parse by row into pandas dataframe
def read_table(path, fields):
    columns = [f['field'] for f in fields]

    # Count str fields to catch rows that were split up in multiple lines
    no_varchar_fields = 0
    for field in fields:
        if field['type'] == 'varchar' or field['type'] == 'char':
            no_varchar_fields += 1

    with open(path, encoding='utf-8') as f:
        rows = f.read().splitlines()
    
    data = []
    hold = ""
    for row in rows:
        hold += row
        if num_bars(hold) >= no_varchar_fields * 2:
            parsed = read_row(hold, fields)
            data.append(parsed)
            hold = ""

    df = pd.DataFrame(data, columns=columns)
    return df