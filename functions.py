import pandas as pd


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
def get_df_from_txt_file(path, fields, bool_fields=None):
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
        'date': 'string',
        'currency': 'Float64'
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
        
        # Process boolean fields
        if bool_fields is not None:
            for field in bool_fields:
                df[field] = df[field].apply(lambda x: x == bool_fields[field]['true'])

    return df


# Takes a processed table and outputs appropriate df's for neo4j csv import
def process_df_neo4j(df, format):
    results = []
    for name, node_format in format['nodes'].items():
        filtered_df = df.loc[:, node_format['columns']]
        if node_format['drop_dupes']:
            filtered_df.drop_duplicates(inplace=True)

        if 'filters' in node_format:
            for key, item in node_format['filters'].items():
                filtered_df = filtered_df.loc[filtered_df[key] == item]
        
        filtered_df.columns = node_format['headers']
        results.append((name, filtered_df))
    
    for name, relationship_format in format['relationships'].items():
        filtered_df = df.loc[:, relationship_format['columns']].drop_duplicates()

        if 'filters' in relationship_format:
            for key, item in relationship_format['filters'].items():
                filtered_df = filtered_df.loc[filtered_df[key] == item]
        
        filtered_df.columns = relationship_format['headers']
        results.append((name, filtered_df))

    return results
