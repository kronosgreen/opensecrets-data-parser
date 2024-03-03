import json
from os.path import join, exists
from os import mkdir
import pandas as pd
import argparse


# Initialize the parser
parser = argparse.ArgumentParser(
                    prog='OpenSecrets Data Parser',
                    description='Parses text files from OpenSecrets.org bulk download ' + \
                                'and converts them to the desired format',
                    epilog='Happy parsing! :)')

# Add arguments
parser.add_argument('data', type=str, help='Path to the directory containing the data files')

# Add optional arguments
parser.add_argument('--output', type=str, default='output', 
                    help='Defines the path to which processed files are written (default: "./output")')
parser.add_argument('--format', choices=['csv', 'neo4j', 'excel'], default='csv', 
                    help='Desired output format (default: csv)')
parser.add_argument('--LobbyOnly', action='store_true', help='Only parse the Lobbying data')

args = parser.parse_args()

# Create output directory if it doesn't exist
if not exists(args.output):
    mkdir(args.output)

print(f"Parsing OpenSecrets data in {args.data} and writing to {args.format} files")


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


# Given columns, read in txt file as a dataframe
def get_df_from_txt_file(path, fields):
    types_parse = {
        'varchar': 'string',
        'text': 'string',
        'char': 'string', 
        'float': 'Float64',
        'memo': 'string',
        'number': 'Float64',
        'int': 'Int32',
        'integer': 'Int32',
        'datetime': 'string',
        'date': 'string'
    }

    dtypes = { field['field'] : types_parse[field['type'].lower()] for field in fields }
    col_names = [field['field'] for field in fields]
    
    parse_dates = [field['field'] for field in fields if 'date' in field['type'].lower()]

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
        # Fix Specific Issue formatting
        if 'lob_issue.txt' in path:
            df.loc[df['SpecificIssue'].str.contains(r"\\"), "SpecificIssue"] = df.loc[
                df['SpecificIssue'].str.contains(r"\\"), "SpecificIssue"
            ].apply(
                lambda x: x.replace('\\' + ' ', '\n').replace('\\', '').rstrip()
            )

    return df


# Read in file data
with open('files/data_dictionary.json') as f:
    data_dictionary = json.load(f)

# Iterate over categories and tables
for category, cat_dict in data_dictionary.items():
    if args.format == 'csv':
        mkdir(f"{args.output}/{category}")
    
    excel_sheets = []

    print("Reading category %s..." % category)
    tables = list(cat_dict['tables'].keys())
    for table in tables:
        print("Reading table %s..." % table)
        path = join(args.data, category, '%s.txt' % table)
        params = cat_dict['tables'][table]
        total_records = params['record_count']

        df = get_df_from_txt_file(path, params['fields'])
        
        if df.shape[0] != total_records:
            error_msg = f"ERROR: {path} has {df.shape[0]} records, but should have {total_records}"
            print(error_msg)
        
        if args.format == 'csv':
            df.to_csv(f"{args.output}/{category}/{table}.csv", index=False)
        elif args.format == 'excel':
            excel_sheets.append((table, df))
        elif args.format == 'neo4j':
            print("Neo4j format not yet supported")

        print("Finished reading %d rows" % df.shape[0])
    
    if args.format == 'excel':
        with pd.ExcelWriter(f"{args.output}/{category}.xlsx") as writer:
            for sheet in excel_sheets:
                sheet[1].to_excel(writer, sheet_name=sheet[0], index=False)
