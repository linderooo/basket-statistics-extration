import fitz
import pandas as pd


def extract_key_value_pairs_from_pdf(pdf_path, page_num=0, box_coords=None, keywords=None):

    key_value_pairs = {}
    with fitz.open(pdf_path) as pdf_doc:
        page = pdf_doc[page_num]
        if box_coords is None:
            box_coords = [(0, 0, page.rect.width, page.rect.height)]  # Full page

        # Combine text extraction for all boxes
        words = [word for box in box_coords for word in page.get_text("words", clip=box)]

        for i, word in enumerate(words):
            clean_word = word[4].rstrip(":")
            # Check keyword match early
            if keywords and clean_word not in keywords:
                continue
            key = clean_word
            # Use slice for efficient value extraction
            value_parts = []
            for next_word in words[i + 1: i + 5]:  # Check at most 10 next words
                clean_next_word = next_word[4].rstrip(":")
                if (keywords and clean_next_word in keywords) or len(value_parts) >= 4:
                    break
                value_parts.append(clean_next_word)
            key_value_pairs[key] = " ".join(value_parts)
    return key_value_pairs

def extract_tables_from_pdf(pdf_path):
    tables = []
    with fitz.open(pdf_path) as doc:
        for page_num in range(len(doc)):
            page = doc[page_num]
            for table in page.find_tables():
                df = table.to_pandas()
                df.dropna(how="all", axis=0, inplace=True)
                df.dropna(how="all", axis=1, inplace=True)
                tables.append(df)
    return tables

def find_four_col_dataframes(dataframes):
    four_col_dfs = []
    count = 0
    for df in dataframes:
        count += 1
        if len(df.columns) == 4:
            new_row = pd.DataFrame([df.columns], columns=df.columns)
            df = pd.concat([new_row, df], ignore_index=True)
            df.columns = ['Score', 'Player', 'Team', 'Event']
            df['Player'] = df['Player'].astype(str).str.lstrip('#')
            df['Event'] = df['Event'].astype(str).str.rstrip('p')
            df_temp = df['Score'].str.split(' - ', expand=True)
            df_temp.columns = ['Home', 'Away']
            df = pd.concat([df, df_temp], axis=1)
            df.drop('Score', axis=1, inplace=True)
            df['Home'] = df['Home'].astype(str).str.strip(' ')
            df['Sort'] = pd.to_numeric(df['Home'], errors='coerce') + pd.to_numeric(df['Away'], errors='coerce')
            four_col_dfs.append(df)

    return four_col_dfs

def find_five_col_dataframes(dataframes, matchnr, hemmalag, bortalag):
    five_col_dfs = []

    for df in dataframes:
        if len(df.columns) == 5:
            df.rename(columns={df.columns[0]: "Player", df.columns[1]: "Starting"}, inplace=True)

            df["Match"] = matchnr

            # Assign 'Lag' here, ensuring that even subsequent dataframes get assigned
            df["Team"] = hemmalag if len(five_col_dfs) == 0 else bortalag

            five_col_dfs.append(df)

    return five_col_dfs

def replace_team_names(events_df: pd.DataFrame, teams_df: pd.DataFrame,
                       player_col='Player', team_col='Team', lag_col='Team') -> pd.DataFrame:

    # Ensure common player values are strings for consistent matching
    teams_df[player_col] = teams_df[player_col].astype(str)
    events_df[player_col] = events_df[player_col].astype(str)

    # Create the mapping dictionary and handle potential missing players gracefully
    team_mapping = teams_df.set_index(player_col)[lag_col].to_dict()

    # Replace team names, keeping original values if no match is found
    events_df[team_col] = events_df[player_col].map(team_mapping).fillna(events_df[team_col])

    return events_df

def sortevents(events,col1,col2):
    count = 0
    num_df = len(events)
    col1_p = col1
    col2_p = col2

    print("events = " + str(num_df))
    print("Column 1 = " + str(col1_p))
    print("Column 2 = " + str(col2_p))

    sorted_dataframes = sorted(events, key=lambda df: df['Sort'].iloc[0])



    for df in sorted_dataframes:
        if num_df == 4:
            count += 1
            df['period'] = count

        elif num_df == 5 and col1 == str(3) and col2 == str(4):
            count += 1
            df['period'] = count
            if count == 4:
                df['period'] = count-1
            elif count == 5:
                df['period'] = count - 1

        elif num_df == 5 and col1 == str(4) and col2 == None:
            count += 1
            df['period'] = count
            if count == 5:
                df['period'] = count-1


    return sorted_dataframes


def get_player_name(player_id, team, df):
    """Extracts the player name based on Player ID and Team.

    Args:
        player_id (int): The ID of the player.
        team (str): The name of the team.
        df (pd.DataFrame): The DataFrame containing the roster data.

    Returns:
        str: The player's name, or None if not found.
    """

    # Filter the DataFrame based on player ID and team
    filtered_df = df[(df['Player'] == player_id) & (df['Team'] == team)]

    # Check if any rows were found
    if not filtered_df.empty:
        return filtered_df['Player name'].iloc[0]  # Return the first matching name
    else:
        return None  # Player not found


def process_allevents(sorted_events, teams, match_info, get_player_name_func):
    """
    Processes  event data, combining, cleaning, and enriching it.

    Args:
        sorted_events: A list of pandas DataFrames containing sorted event data.
        teams: A dictionary or DataFrame containing team information.
        match_info: A dictionary containing information about the match (competition, arena, match number).
        get_player_name_func: A function that takes 'Player' and 'Team' values and returns the full player name.

    Returns:
        A pandas DataFrame containing the processed event data.
    """

    allevents = pd.concat(sorted_events, ignore_index=True)

    # Prepare for Player name insertion
    allevents.insert(1, "Player name", True)
    allevents['Player name'] = allevents['Player name'].astype(str)

    # Remove unnecessary 'Sort' column
    allevents.drop('Sort', axis=1, inplace=True)

    # Enrich event data with player names and match information
    for index, row in allevents.iterrows():
        player_name = get_player_name_func(row['Player'], row['Team'], teams)
        allevents.loc[index, 'Player name'] = player_name
        allevents.loc[index, 'Competition'] = match_info["Competition"]
        allevents.loc[index, 'Arena'] = match_info["Arena"]
        allevents.loc[index, 'Matchnr'] = match_info["Matchnr"]

    return allevents

def get_all_events(pdf_path, lag_coords, competition_coords, date_coords, arena_coords, matchnr_coords):
    """Extracts and processes all events from a PDF report.

    Args:
        pdf_path (str): Path to the PDF report.
        lag_coords, competition_coords, ... (tuple): Coordinates for extracting match info.

    Returns:
        pd.DataFrame: A DataFrame containing all processed events.
    """

    match_info = extract_key_value_pairs_from_pdf(
        pdf_path,
        box_coords=[lag_coords, competition_coords, date_coords, arena_coords, matchnr_coords],
        keywords=["Hemmalag", "Bortalag", "Competition", "time", "Arena", "Matchnr"]
    )

    tables, _ = extract_tables_from_pdf(pdf_path)  # We don't need the titles in this function
    team = find_five_col_dataframes(tables, match_info["Matchnr"], match_info["Hemmalag"], match_info["Bortalag"])
    teams = pd.concat(team, ignore_index=True)
    events = find_four_col_dataframes(tables, []) # Empty list for titles

    # Sort and process events within the function itself
    sorted_events = sortevents(events)
    updated_events_df = replace_team_names(sorted_events[0], teams)

    for event in sorted_events:
        event['Team'] = event['Team'].replace({'Lag A': match_info["Hemmalag"], 'Lag B': match_info["Bortalag"]})

    return process_allevents(sorted_events, teams, match_info, get_player_name)  # Utilize the provided helper function

