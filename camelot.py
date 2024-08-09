import camelot
import pandas as pd
import re
import sys


def extract_tables_with_player_name(pdf_file, search_term="Player name", target_column=2):
    tables = camelot.read_pdf(pdf_file, flavor='lattice', line_scale=100)
    matching_dataframes = []

    for i, table in enumerate(tables):
        try:
            df = table.df
            if target_column < len(df.columns):
                if any(df.iloc[:, target_column].astype(str).str.contains(search_term, flags=re.IGNORECASE,
                                                                          regex=True)):
                    df.columns = ['Number', 'Starting', 'Player name', 'Points', 'Fouls']

                    # Remove text after 'Ø' in 'Player name'
                    df['Player name'] = df['Player name'].astype(str).str.split('Ø', n=1).str[0]

                    # Check and modify rows starting with 'Ø'
                    mask = df['Player name'].astype(str).str.startswith('Ø')
                    df.loc[mask, 'Player name'] = (
                            df.loc[mask, 'Player name'].astype(str).str[1:]
                            + ' '
                            + df.loc[mask, 'Starting'].astype(str)
                    )
                    # Copy 'Starting' to 'Player name' before replacement
                    df['Player name'] = df['Player name'].astype(str) + ' ' + df['Starting'].astype(str)
                    df['Player name'] = df['Player name'].astype(str).str.lstrip(' Ø')
                    df['Player name'] = df['Player name'].astype(str).str.rstrip(' ')

                    # Replace 'Starting' values with booleans
                    df['Starting'] = df['Starting'].astype(str).str.contains('Ø')
                    matching_dataframes.append(df.copy())


        except camelot.ReadPdfError as e:
            print(f"Error reading table {i} from PDF: {e}")
        except Exception as e:  # Catch other potential errors
            print(f"Error processing table {i}: {e}")

    return matching_dataframes


def extract_events(pdf_file, search_term=" - ", target_column=0):
    tables = camelot.read_pdf(pdf_file, flavor='lattice', line_scale=100)
    matching_dataframes = []

    for i, table in enumerate(tables):
        try:
            df = table.df
            if target_column < len(df.columns):
                if any(df.iloc[:, target_column].astype(str).str.contains(search_term, flags=re.IGNORECASE,
                                                                          regex=True)):
                    df.columns = ['Score', 'Player', 'Team', 'Event']

                    df['Player'] = df['Player'].astype(str).str.lstrip('#')
                    df_temp = df['Score'].str.split(' - ', expand=True)
                    # Name the new columns.
                    df_temp.columns = ['Home', 'Away']

                    # Concatenate the original and temporary DataFrames horizontally.
                    df = pd.concat([df, df_temp], axis=1)

                    # Remove the original column.
                    df.drop('Score', axis=1, inplace=True)
                    df['Home'] = df['Home'].astype(str).str.strip(' ')
                    df['Sort'] = pd.to_numeric(df['Home'], errors='coerce') + pd.to_numeric(df['Away'], errors='coerce')
                    matching_dataframes.append(df.copy())




        except camelot.ReadPdfError as e:
            print(f"Error reading table {i} from PDF: {e}")
        except Exception as e:  # Catch other potential errors
            print(f"Error processing table {i}: {e}")

    return matching_dataframes


if __name__ == "__main__":
    pdf_file = "/app/data/report.pdf"
    teams = extract_tables_with_player_name(pdf_file)
    hometeam = teams[0].iloc[1:, :]
    awayteam = teams[1].iloc[1:, :]
    event_dataframes = extract_events(pdf_file)