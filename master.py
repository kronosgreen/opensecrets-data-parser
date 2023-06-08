import json
from os.path import join
import pandas as pd

# Read in files 
with open('data_dictionary.json') as f:
    data_dictionary = json.load(f)

categories = list(data_dictionary.keys())


def get_col_info(fields):
    types_parse = {
        'varchar': 'string',
        'text': 'string',
        'char': 'string', 
        'float': 'Float64',
        'number': 'Float64',
        'int': 'Int32',
        'integer': 'Int32',
        'datetime': 'string'
    }
    dtypes = { field['field'] : types_parse[field['type'].lower()] for field in fields}
    col_names = [field['field'] for field in fields]

    return (col_names, dtypes)


for category, cat_dict in data_dictionary.items():
    if category == "Lobby":
        continue
    print("Reading category %s..." % category)
    tables = list(cat_dict['tables'].keys())
    for table in tables:
        print("Reading table %s..." % table)
        path = join('data/', category, '%s.txt' % table)
        params = cat_dict['tables'][table]
        # df = get_df_from_txt(path, params)
        total_records = params['record_count']
        col_names, dtypes = get_col_info(params['fields'])
        parse_dates = [col for col, dtype in dtypes.items() if dtype == 'datetime64']
        df = pd.read_csv(path,
                header=None,
                dtype=dtypes,
                parse_dates=parse_dates,
                names=col_names,
                quotechar='|',
                encoding_errors='ignore')
        
        assert df.shape[0] == total_records

        print("Finished reading %d rows" % df.shape[0])