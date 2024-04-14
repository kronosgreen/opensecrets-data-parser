import json
from os.path import join, exists
from os import mkdir
import pandas as pd
from functions import get_df_from_txt_file, process_df_neo4j
import argparse

# Constants
EXCEL_ROW_LIMIT = 1000000

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
parser.add_argument('--PFDOnly', action='store_true', help='Only parse the personal finance data')
parser.add_argument('--ExpenditureOnly', action='store_true', help='Only parse the expenditure data')
parser.add_argument('--FiveTwoSevenOnly', action='store_true', help='Only parse the 527 data')
parser.add_argument('--CampaignFinOnly', action='store_true', help='Only parse the campaign finance data')

args = parser.parse_args()

# Create output directory if it doesn't exist
if not exists(args.output):
    mkdir(args.output)

# Read in file data
with open('files/data_dictionary.json') as f:
    data_dictionary = json.load(f)

neo4j_dictionary = {}
if args.format == 'neo4j':
    with open('files/neo4j-headers.json') as f:
        neo4j_dictionary = json.load(f)

# Iterate over categories and tables
print(f"Parsing OpenSecrets data in {args.data} and writing to {args.format} files")
for category, cat_dict in data_dictionary.items():

    if args.LobbyOnly and category != 'Lobby':
        continue
    if args.PFDOnly and category != 'PFD':
        continue
    if args.ExpenditureOnly and category != 'Expenditures':
        continue
    if args.CampaignFinOnly and category != 'CampaignFin':
        continue
    if args.FiveTwoSevenOnly and category != '527':
        continue

    if args.format in ['csv', 'neo4j'] and not exists(f"{args.output}/{category}"):
        mkdir(f"{args.output}/{category}")

    excel_sheets = []

    print("Reading category %s..." % category)
    tables = list(cat_dict['tables'].keys())
    for table in tables:
        print("Reading table %s..." % table)
        path = join(args.data, category, '%s.txt' % table)
        params = cat_dict['tables'][table]
        total_records = params['record_count']

        bool_fields = params['boolean_fields'] if 'boolean_fields' in params else None

        df = get_df_from_txt_file(path, params['fields'], bool_fields)
        
        if df.shape[0] != total_records:
            error_msg = f"ERROR: {path} has {df.shape[0]} records, but should have {total_records}"
            print(error_msg)
        
        if args.format == 'csv':
            df.to_csv(f"{args.output}/{category}/{table}.csv", index=False)
        elif args.format == 'excel':
            if df.shape[0] >= 1048576:
                print("WARNING: Excel only supports 1,048,576 rows. Splitting up file.")
                ind = 1
                for i in range(0, df.shape[0], EXCEL_ROW_LIMIT):
                    excel_sheets.append((f"{table}_{ind}", df[i:i+EXCEL_ROW_LIMIT]))
                    ind += 1
            else:
                excel_sheets.append((table, df))
        elif args.format == 'neo4j':
            results = process_df_neo4j(df, neo4j_dictionary[category][table])
            for r, name in results:
                r.to_csv(f"{args.output}/{category}/{table}/{name}.csv", index=False)

        print("Finished reading %d rows" % df.shape[0])
    
    if args.format == 'excel':
        with pd.ExcelWriter(f"{args.output}/{category}.xlsx") as writer:
            for sheet in excel_sheets:
                sheet[1].to_excel(writer, sheet_name=sheet[0], index=False)
