import sys
from functions import *

lag = (50, 140, 500, 145)
competition = (50, 160, 380, 170)  # Coordinates for the first box
date = (50, 185, 200, 200)  # Coordinates for the first box
arena = (120, 185, 360, 190)  # Coordinates for the first box
matchnr = (380, 185, 600, 190)  # Coordinates for the second box
Column1 = (250, 240, 360, 700)  # Coordinates for the second box
Column2 = (400, 240, 520, 700)  # Coordinates for the second box

def process_pdfs(pdf_paths):
    for pdf in pdf_paths:

        match_info = extract_key_value_pairs_from_pdf(pdf, box_coords=[lag,competition,date,arena,matchnr], keywords=["Hemmalag", "Bortalag", "Competition", "time", "Arena", "Matchnr"])
        event_col1 = extract_key_value_pairs_from_pdf(pdf, box_coords=[Column1], keywords=["Start",])
        event_col2 = extract_key_value_pairs_from_pdf(pdf, box_coords=[Column2], keywords=["Start",])

        col1_stat = event_col1.get("Start")  # Get the value, or None if "Start" isn't there
        if col1_stat:
            col1_stat = col1_stat.split()[1]

        col2_stat = event_col2.get("Start")  # Get the value, or None if "Start" isn't there
        if col2_stat:
            col2_stat = col2_stat.split()[1]

        print(match_info)

        tables = extract_tables_from_pdf(pdf)
        team = find_five_col_dataframes(tables, match_info["Matchnr"], match_info["Hemmalag"], match_info["Bortalag"])
        teams = pd.concat(team, ignore_index=True)
        events = find_four_col_dataframes(tables)
        sorted = sortevents(events,col1_stat,col2_stat)
        updated_events_df = replace_team_names(sorted[0], teams)
        for event in sorted:
            event['Team'] = event['Team'].replace('Lag A', match_info["Hemmalag"])
            event['Team'] = event['Team'].replace('Lag B', match_info["Bortalag"])
        allevents = pd.concat(sorted, ignore_index=True)
        allevents.insert(1, "Player name", True)
        allevents['Player name'] = allevents['Player name'].astype(str)
        allevents.drop('Sort', axis=1, inplace=True)

        for index, row in allevents.iterrows():

            player_name = get_player_name(row['Player'], row['Team'], teams)

            allevents.loc[index, 'Player name'] = player_name
            allevents.loc[index, 'Competition'] = match_info["Competition"]
            allevents.loc[index, 'Arena'] = match_info["Arena"]
            allevents.loc[index, 'Matchnr'] = match_info["Matchnr"]


        allevents.to_csv((match_info["Matchnr"]+'.csv'), index=False)  # Add this line

def main():
  pdf_paths = sys.argv[1:]
  process_pdfs(pdf_paths)

if __name__ == "__main__":
  main()