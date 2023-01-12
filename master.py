import json
from os.path import join
from helpers.functions import read_table

# Read in files 
with open('data_dictionary.json') as f:
    data_dictionary = json.load(f)

categories = list(data_dictionary.keys())

for category in categories:
    category_dict = data_dictionary[category]
    tables = list(category_dict.keys())
    for table in tables:
        print("Reading in table %s..." % table)
        path = join('data/', category, '%s.txt' % table)
        params = category_dict[table]
        total_records = params['record_count']
        fields = params['fields']
        
        df = read_table(path, fields)
        
        # Check if parsed dataframe matches record count
        if df.shape[0] != total_records:
            print("DataFrame (%d rows) did not match record count (%d)" % (df.shape[0], total_records))
        
        print(df)
