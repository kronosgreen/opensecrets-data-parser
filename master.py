import json
from os.path import join
import pandas as pd

# Read in file data
with open('data_dictionary.json') as f:
    data_dictionary = json.load(f)
    categories = list(data_dictionary.keys())


# lob_lobbyist data is a special case
# some records are split across multiple lines
def parse_lob_lobbyist(path, columns):
    parsed_data = []
    bad_data = []
    with open(path, 'rb') as f:
        lines = f.read().splitlines()
        for i, line in enumerate(lines):
            line_data = line.decode(errors='ignore').split('|')
            line_parsed = line_data[1::2]
            if len(line_parsed) != 8:
                bad_data.append(line.decode(errors='ignore'))
                continue
            else:            
                parsed_data.append(line_parsed)
    
    temp_line = ""
    parsed_bad_data = []
    for line in bad_data:
        temp_line += line
        if len(temp_line.split('|')[1::2]) == 8:
            parsed_bad_data.append(temp_line.split('|')[1::2])
            temp_line = ""

    df = pd.DataFrame(parsed_data + parsed_bad_data, columns=columns)

    return df


def get_df_from_txt_file(path, fields):
    types_parse = {
        'varchar': 'string',
        'text': 'string',
        'char': 'string', 
        'float': 'Float64',
        'number': 'Float64',
        'int': 'Int32',
        'integer': 'Int32',
        'datetime': 'string',
        'date': 'string'
    }

    dtypes = { field['field'] : types_parse[field['type'].lower()] for field in fields }
    col_names = [field['field'] for field in fields]
    
    parse_dates = [field['field'] for field in fields if field['type'].lower().contains('date')]

    if 'lob_lobbyist' in path:
        df = parse_lob_lobbyist(path, col_names)
    else:
        df = pd.read_csv(path,
                engine='python',
                header=None,
                dtype=dtypes,
                parse_dates=parse_dates,
                names=col_names,
                quotechar='|',
                encoding_errors='ignore'
        )

    return df


for category, cat_dict in data_dictionary.items():
    print("Reading category %s..." % category)
    tables = list(cat_dict['tables'].keys())
    for table in tables:
        print("Reading table %s..." % table)
        path = join('data/', category, '%s.txt' % table)
        params = cat_dict['tables'][table]
        total_records = params['record_count']

        df = get_df_from_txt_file(path, params['fields'])
        
        if df.shape[0] != total_records:
            print(f"ERROR: {path} has {df.shape[0]} records, but should have {total_records}")

        print("Finished reading %d rows" % df.shape[0])