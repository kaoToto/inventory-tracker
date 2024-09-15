from collections import defaultdict
from pathlib import Path
import sqlite3

import streamlit as st
import altair as alt
import pandas as pd
import base64, json
import requests
from io import StringIO
import numpy as np
import re
import unicodedata
from datetime import datetime

import io

# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title="NWO transfers",
    page_icon=":recycle:",  # This is an emoji shortcode. Could be a URL too.
)


# Clan list 

link_template = "https://www.armyneedyou.com/team/user_export?type=current&dateType=lastday&token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9."

clan_names = {}
clan_names[ 835 ] = "NWO"
clan_names[ 47257] = "RES1"
clan_names[ 10283] = "RES2"
clan_names[ 115] = "RES3"
clan_names[ 4023] = "RES5"
clan_names[ 120420] = "RES6"
clan_names[ 3037] = "RES7"
clan_names[ 140409] = "BRA1"
clan_names[ 8961] = "BRA2"
clan_names[ 96873] = "BRA3"
clan_names[ 103475] = "BRA4"
clan_names[ 111] = "BRA5"
clan_names[ 434] = "BRA6"
clan_names[ 4200] = "BRA7"
clan_names[ 5425] = "SH1"
clan_names[ 143430] = "SH2"
clan_names[ 133909] = "SH3"
clan_names[142364] = "ARB"

# lets populate a reverse table to ease lookups
clan_ids = {}
for k, v in clan_names.items():
    family = re.sub(r'\d+', '',v)
    # add new families
    if family not in clan_ids.keys():
        clan_ids[family]= [ k ]
    else :
        clan_ids[family].append(k)




possible_families = list(clan_ids.keys())
possible_families  = possible_families[1:] # all teams but NWO
 
# Function to normalize and replace fancy letters
def replace_fancy_letters(text, remove_numbers=False):
    # Normalize to NFKD form (compatibility decomposition)
    normalized_text = unicodedata.normalize('NFKD', text)
    # Filter out non-ASCII characters
    ascii_text = ''.join([char for char in normalized_text if char.isascii()])

    #remove whitespaces
    ascii_text = re.sub(r'\s+', '', text)
    if remove_numbers:
        #remove numbers
        ascii_text = re.sub(r'\d+', '', ascii_text )

    # Convert to uppercase
    return ascii_text.upper()


# -----------------------------------------------------------------------------
# Base 64 decode

def base64_padding(source_str): 
    """ Base 64 has = or == endings in standard and our strings don't have them, hence we need a conversion"""
    pad = {0: "", 2: "==", 3: "="}

    mod4 = (len(source_str)) % 4
    if (mod4 == 1):
        raise ValueError("Invalid input: its length mod 4 == 1")
    b64u = source_str + pad[mod4]
    padded_str = base64.b64decode(b64u)
    return padded_str

if False:
    # example to b64decode a link clan part 
    linksEnd = "eyJ1aWQiOiIxNDA0MDkifQ", 
    print(json.loads(base64_padding(linkEnd).decode("utf8"))["uid"])



# -----------------------------------------------------------------------------
# Declare some useful db functions.


def connect_db():
    """Connects to the sqlite database."""

    DB_FILENAME = Path(__file__).parent / "players.db"
    db_already_exists = DB_FILENAME.exists()

    conn = sqlite3.connect(DB_FILENAME)
    db_was_just_created = not db_already_exists

    return conn, db_was_just_created


def initialize_data(conn):
    """Initializes the players table with no data."""
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY,
            player_name TEXT,
            clan TEXT
        )
        """
    )
    conn.commit()

def add_generals(conn):
    """ migration : Initializes the generals table with some data. """
    empty_general_list = ""
    for k, _ in clan_names.items():
        empty_general_list = f""" {empty_general_list}
        ({k},0),"""


    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS generals (
            clan_id INTEGER PRIMARY KEY,
            player_id INTEGER
        )
        """
    )

    query = f"""
        INSERT OR IGNORE INTO 'generals'
            (clan_id , player_id)
        VALUES
            {empty_general_list.strip(",")};
        """

    cursor.execute( query )
    conn.commit()

def add_new_player(id, name):
    cursor = conn.cursor()
    cursor.execute(
        f"""
        INSERT OR IGNORE INTO players
            (player_id , player_name,  clan)
        VALUES
            ({id}, '{name}','')
        """
    )
    conn.commit()

def add_new_general(clan_id, player_id):
    cursor = conn.cursor()
    cursor.execute(
        f"""
        INSERT OR IGNORE INTO generals
            (clan_id , player_id)
        VALUES
            ({clan_id}, {player_id})
        """
    )
    conn.commit()

def add_or_update_player(conn, id, name,clan):
    cursor = conn.cursor()
    cursor.execute(
        f"""
        INSERT OR IGNORE INTO players
            (player_id , player_name,  clan)
        VALUES
            ({id}, '{name}',  '{clan}')
        """
    )

    cursor.execute(  
        f"""
        UPDATE players 
        SET
            player_name = '{name}', clan = '{clan}'
        WHERE 
            player_id = {id} 
        """
    )
    conn.commit()

def add_or_update_general(clan_id, player_id):
    cursor = conn.cursor()
    cursor.execute(
        f"""
        INSERT OR IGNORE INTO generals
            (clan_id , player_id)
        VALUES
            ({clan_id}, {player_id})
        """
    )

    cursor.execute(  
        f"""
        UPDATE generals 
        SET
            player_id = {player_id}
        WHERE 
            clan_id = {clan_id} 
        """
    )
    conn.commit()



def load_players_data(conn):
    """Loads the players data from the database."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM players")
        data = cursor.fetchall()
    except:
        return None

    df = pd.DataFrame(
        data,
        columns=[
            "player_id",
            "player_name",
            "clan",
        ],
    )
    return df

def clan_name_for_id (row):
    if 'clan_id' in row and row['clan_id'] in clan_names.keys():
        return clan_names[row['clan_id']]
    return ''

def player_name_for_id (row, playerdf):
    if 'player_id' in row and playerdf[playerdf['player_id'] == row['player_id']]:
        line = playerdf[playerdf['player_id'] == row['player_id']]
        return line["Name"].values[0]          
    return ''

def load_generals_data(conn):
    """Loads the generals data from the database."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM generals")
        data = cursor.fetchall()
    except Exception as e:
        print(e)
        return None

    df = pd.DataFrame(
        data,
        columns=[
            "clan_id",
            "player_id"
        ],
    )
    df['clan_name'] =  df.apply(clan_name_for_id ,axis =1)

    #drop rows that do not correspond to a listed clan (no name)
    df = df[df['clan_name'] != '']
    return df


def update_players_data(conn, df, changes):
    """Updates the players data in the database."""
    cursor = conn.cursor()

    if changes["edited_rows"]:
        deltas = st.session_state.player_table["edited_rows"]
        rows = []

        for i, delta in deltas.items():
            row_dict = df.iloc[i].to_dict()
            row_dict.update(delta)
            #make it latin letters uppercase
            row_dict["clan"] = replace_fancy_letters(row_dict["clan"], remove_numbers=True)

            if row_dict["clan"]  is None or row_dict["clan"] in possible_families :
                rows.append(row_dict)
            else :
                st.toast(f"Invalid clan of origin `{row_dict['clan']}` for player {row_dict['player_name']}. Value must be in {str(possible_families)}")


        cursor.executemany(
            """
            UPDATE players
            SET
                player_name = :player_name,
                clan = :clan
            WHERE player_id = :player_id
            """,
            rows,
        )

    if changes["added_rows"]:
        cursor.executemany(
            """
            INSERT INTO players
               ( player_id, player_name , clan)
            VALUES
                (:player_id, :player_name, :clan)
            """,
            (defaultdict(lambda: None, row) for row in changes["added_rows"]),
        )

    if changes["deleted_rows"]:
        cursor.executemany(
            "DELETE FROM players WHERE player_id = :player_id",
            ({"player_id": int(df.loc[i, "player_id"])} for i in changes["deleted_rows"]),
        )

    conn.commit()


# -----------------------------------------------------------------------------
# Draw the actual page, starting with the players table.

# Set the title that appears at the top of the page.
"""
# :recycle: NWO transfers

**Welcome to NWO transfers site**

*This page prepares NWO clan movements.*

"""

with st.expander("How to use?"):
    st.markdown("""


1. *[Optional]* In the first table, set a clan of origin for NWO players that are not identifed as BRA / SH / RES. Those without orgin clan will be first
2. **[Compulsory]** Make sure you have the correct list of generals
3. Press the load button to download and process last reset's reports. They are available 2-3 hours after reset

    You get two tables, one with the full player list sorted by rank. A second with all moves
    You may edit the destination clan in the second table, please check that you do not overfill a clan above 50
    You may also download any of the tables as csv to do whatever you want with it in excel

4.  The full transfer list in the format requested by the devs is then available for download in excel format.
""")

st.subheader("Clans of Origin")
st.info(
    """
    Use the following table to set a team (RES, BRA, SH) of origin for all NWO players.
    When the player no longer qualifies for NWO, he will be sent to his clan of origin.
    If no clan of origin is set, one will be granted randomly.
    """
)

# Connect to database and create table if needed
conn, db_was_just_created = connect_db()

# Initialize data.
if db_was_just_created:
    initialize_data(conn)
    st.toast("Database initialized with some sample data.")

add_generals(conn)

# Load data from database
players_df = load_players_data(conn)
players_df = players_df.sort_values(by='clan', ascending=True)

generals_df = load_generals_data(conn)

col1,col2 = st.columns([3,2])

with col1:
# Display data with editable table
    edited_df = st.data_editor(
        players_df,
        num_rows="dynamic",  # Allow appending/deleting rows.
        column_config={
            # Show dollar sign before price columns.
            #"price": st.column_config.NumberColumn(format="$%.2f"),
            #"cost_price": st.column_config.NumberColumn(format="$%.2f"),
        },
        key="player_table",
    )
    has_uncommitted_changes = any(len(v) for v in st.session_state.player_table.values())

with col2:
    st.button(
        ":warning: Save Changes",
        type="primary",
        disabled=not has_uncommitted_changes,
        # Update data in database
        on_click=update_players_data,
        args=(conn, players_df, st.session_state.player_table),
    )

st.subheader("Generals")

st.info("It is mandatory to have un up to date list of generals")

col1,col2 = st.columns([3,2])
generals_df['priority'] = generals_df['clan_name'].apply(lambda x: f"0{x}" if x.startswith('NW') else f"1{x}" )

generals_df =  generals_df.sort_values(by='priority')
generals_df.drop(columns=['priority'])
generals_df.reset_index(inplace=True)

generals_df['player_name']= generals_df['player_id'].apply( lambda player_id :  '' if player_id == 0 else players_df[ players_df['player_id'] == player_id]['player_name'].values[0] )

# Display data withnot editable
with col1: 
    st.dataframe(
        generals_df,
        column_order = ("clan_id", "clan_name",   "player_id",  "player_name"),   
    )
                
with col2: 
    with st.container(border=True):
        st.write("Change a General")
        generals_have_uncommitted_changes = False
        last_gen_clan = None
        last_gen_name = None

        new_general_clan = st.selectbox("Clan",options = clan_names.values(), index = None)
        new_general_name = st.selectbox("General",options =players_df['player_name'], index = None)

        if st.button(":warning: Save Changes",
                     disabled = not new_general_clan  or not new_general_name
            , key="generalbutt"):
            new_general_id = players_df[ players_df['player_name'] == new_general_name ]['player_id'].values[0]
            new_general_clan_id = None
            for k,v in clan_names.items():
                if v == new_general_clan :
                    new_general_clan_id = k
                    break
            last_gen_clan = new_general_clan
            last_gen_name = new_general_name      
            add_or_update_general(clan_id=new_general_clan_id, player_id=new_general_id)

            st.rerun()
        

                    




st.subheader("Pull last reset reports from aow")
st.info(
    """
    Use the following button to pull last available reset ranks from AOW and edit movements
    """
)
def get_clan_string(clan_id):
    return f'{{"uid":"{clan_id}"}}'

def get_last_day_link(clan_id):
    clan_string =get_clan_string(clan_id)
    clan_string_encoded = base64.b64encode(clan_string.encode("utf8")).decode("utf8").rstrip('=')
    url = f"{link_template}{clan_string_encoded}.NWOTOKEN&battle=pvp"
    print(url)
    return url


 

def pull_all_links(clan_tag):
    target_df=  pd.DataFrame() 
    for clan_id in clan_ids [clan_tag]: 
        url = get_last_day_link(clan_id)
        response = requests.get(url)
        csv_data = response.content.decode('utf-8')
        csv_file = StringIO(csv_data)
        local_df = pd.read_csv(csv_file)
        local_df = local_df.iloc[:-1]
        local_df['Team'] = clan_tag
        local_df['Current Clan'] = clan_id
        local_df['Current Clan Name'] = clan_names[clan_id] if clan_id in clan_names else ""
        target_df = pd.concat( [target_df,local_df], ignore_index=True)
        
    return target_df
    
def assign_users(df):
    for _, row in df.iterrows():
        id = row['ID']
        name = row['Name']
        team = row['Team']
        add_or_update_player(conn, id,name,team)

def create_new_users(df):
    for _, row in df.iterrows():
        id = row['ID']
        name = row['Name']
        add_new_player(id,name)


# Function to fill missing values with a random choice from possible_values
def fill_missing_values(row):

    matching_row = players_df[players_df['player_id'] - row['ID'] == 0]
    clan = matching_row['clan'].values[0] if not matching_row.empty else None
    if clan and len(clan) > 0:
        return clan
    else :
        print(f"not found {row['ID']}  {row['Current Clan Name']} ")
        #print(matching_row)
    return np.random.choice(possible_families)
    

if st.button("Reload players ranks from NWO"):
    df_sh = pull_all_links("SH")
    assign_users(df_sh)
    df_res = pull_all_links("RES")
    assign_users(df_res)
    df_bra = pull_all_links("BRA")
    assign_users(df_bra)

    df_nwo= pull_all_links("NWO")
    create_new_users(df_nwo)

    st.session_state.players_df = pd.concat ([df_sh,df_res,df_bra,df_nwo])
    st.session_state.players_df = st.session_state.players_df.sort_values(by='Trophies', ascending=False)
    print("before apply len",len(st.session_state.players_df))
    st.session_state.players_df['ID'] =  pd.to_numeric(st.session_state.players_df['ID'], errors='coerce')

    st.session_state.players_df['origin'] = st.session_state.players_df.apply(fill_missing_values, axis=1)
    print("after apply len",len(st.session_state.players_df))

    st.rerun()

if "players_df" in st.session_state:
    st.subheader("Full player list")
    #st.dataframe(st.session_state.players_df)
    ## All players
    st.dataframe(st.session_state.players_df )
    
    
    st.session_state.moves = ""
    ## Moves to NWO
    print(clan_ids["NWO"])

    nwo_clan_count = len( clan_ids["NWO"])
    st.session_state.movesdf = pd.DataFrame(columns=[
            "player_id",
            "from",
            "destination",
            "player_name",
            "origin",
            "current team",
            "dest team", 
            "rank"]
            )
    
    # we will sort 49 player per team, on a list with no generals
    # unfortunately,  if a selected general is not in his clan,
    # we would not consider and he will be not counted in the clan where he is
    # which would end at 51
    # and  counted in the clan he should be, where he is no more
    # which would end at 49

    # to prevent that, we need to sort 49 player per clan and ignore one player, preferably in the middle of the clan

    # so, we will make a list of 1 player per clan
    # with all generals that are at home, and a middle ranked player from that clan otherwise 

    # Merge player_df with general_df on player_id

   
    st.session_state.players_df['player_id'] = st.session_state.players_df['ID']
    merged_df = generals_df.merge(st.session_state.players_df[['player_id', 'Name', 'Current Clan']], 
                             on= 'player_id', 
                             how='left', 
                             )
    
    # Create 'at_home' column where clan_id in general_df matches clan_id in player_df
    merged_df['at_home'] = merged_df['clan_id'] == merged_df['Current Clan']
    # Replace NaN values in the 'Name' column with an empty string
    merged_df['Name'] = merged_df['Name'].fillna('?')
    merged_df['name_clan'] = merged_df['Name'] + ' (' + merged_df['clan_name'] + ')'
    wandering_generals = merged_df[  merged_df['at_home']  == False ]
    if not wandering_generals['name_clan'].empty:
        st.warning(f"The following listed Generals are not even in their clan {str(wandering_generals['name_clan'].tolist())}")
    def find_middle_player(clan_id):
        # Filter player_df by clan_id
        clan_players = st.session_state.players_df[st.session_state.players_df['Current Clan'] == clan_id]
        
        if not clan_players.empty:
            # Get the middle index
            middle_index = len(clan_players) // 2
            # Select the player at the middle index
            middle_player = clan_players.iloc[middle_index]
            return middle_player['player_id']
        else:
            return None  # No players in this clan
        
    def replace_general(general_row):
        if general_row['at_home']:
            return general_row['player_id']  # Keep original general if at home
        else:
            return find_middle_player(general_row['clan_id'])  # Replace if not at home
        
    
    # Apply the function to find replacements for non-home generals
    merged_df['new_general'] = merged_df.apply(replace_general, axis=1)
    st.session_state.players_df['cant_move'] = st.session_state.players_df['ID'].apply( lambda player_id :  player_id in merged_df['new_general'].tolist() )
    
    players_excepted_generals_df = st.session_state.players_df[st.session_state.players_df['cant_move']  == False]
    for index, nwo_clan_id in enumerate(clan_ids["NWO"]):
        for _, row in players_excepted_generals_df.iloc[49*index:49+49*index].iterrows():
            #if row["Current Clan"] != nwo_clan_id:
            #st.session_state.movesdf =  f"{st.session_state.moves}\n{row["ID"]} - {row["Current Clan"]} - {nwo_clan_id}"
            
            # New row to add
            new_row = {"player_id" : row["ID"],
                "from": row["Current Clan"],
                "destination": nwo_clan_id,
                "player_name": row["Name"],
                "current team":row["Current Clan Name"],
                "origin":row["origin"],
                "dest team" : clan_names[nwo_clan_id] if nwo_clan_id in  clan_names else nwo_clan_id,
                "rank" : row["Ranking"]
            }
            # Add the new row using loc
            st.session_state.movesdf.loc[len(st.session_state.movesdf )] = new_row

    remaining_players_df = players_excepted_generals_df.iloc[49*nwo_clan_count:]
    
    clans_to_sort = possible_families
    for clan_to_sort in clans_to_sort: 
    
        remaining_players_clan = remaining_players_df[remaining_players_df['origin'] == clan_to_sort]
        for index, target_clan_id in enumerate(clan_ids[clan_to_sort]):
            for _, row in remaining_players_clan[49*index:49+49*index].iterrows():
                #if row["Current Clan"] != target_clan_id:
                # New row to add
                new_row = {"player_id" : row["ID"],
                    "from": row["Current Clan"],
                    "destination": target_clan_id,
                    "player_name": row["Name"],
                    "current team":clan_names[row["Current Clan"]] if row["Current Clan"] in  clan_names else "",
                    "origin":row["origin"],
                    "dest team" : clan_names[target_clan_id] if target_clan_id in  clan_names else clan_to_sort,
                    "rank" : row["Ranking"]
                    }
                # Add the new row using loc
                st.session_state.movesdf.loc[len(st.session_state.movesdf )] = new_row

def check_dest_team(row):
    new_dest_team = replace_fancy_letters(row["dest team"])
    key = None
    for k, v in clan_names.items():
        if v == new_dest_team:
            key = k
            break 
    if key is None :
        st.toast(f"Unknown team {new_dest_team}", icon="🚨")
    #print(key, row["dest team"])
    return key



if "movesdf" in st.session_state.keys() :
    ## Moves
    st.subheader("Moves")
    st.info("""
    Use this table to check moves and edit dest team.
    The table below will be recalculated after each edit, 
    make sure that clans are full but not over 50. 
            
    Generals cannot be moved, they are not in the table
    """)
    filter_moves_only = st.checkbox("Filter on moves only")
   
    filtered_moves_df= st.session_state.movesdf[ st.session_state.movesdf['from'] !=  st.session_state.movesdf['destination'] ]  if filter_moves_only else st.session_state.movesdf
    edited_movesdf = st.data_editor(
        filtered_moves_df,
        num_rows="dynamic",
        disabled=("player_id",
            "from",
            "destination",
            "player_name",
            "origin",
            "current team","rank"),
        column_order = ("player_id",
            "from",
            "destination",
            "player_name",
            "origin", 
            "current team","dest team" ,"rank")
        ) 
    if edited_movesdf is not None  and not edited_movesdf.equals(st.session_state.movesdf):
        edited_movesdf['destination']=edited_movesdf.apply(check_dest_team,axis=1)
        edited_movesdf['dest team']=edited_movesdf['dest team']
        st.session_state.movesdf = edited_movesdf
    
    st.subheader("Players per team")
    st.info("""
    Count of players in each of our teams if we do the transfers        
    """)

    col1,col2,col3,col4= st.columns([1,1,1,1])
    cols = [col1, col2, col3, col4]
    index = 0 
    for key in clan_ids.keys():
        with cols[index % len(cols)]:
            index += 1
            ldf = edited_movesdf[edited_movesdf['dest team'].str.contains(key, case=False, na=False)]["dest team"].value_counts() +1 
            st.dataframe(ldf)
    

    export_df = st.session_state.movesdf[ st.session_state.movesdf['from'] !=  st.session_state.movesdf['destination'] ] 
    export_df = export_df.sort_values(by=['from', 'destination'])   
    export_df = export_df.rename(columns={'from': 'from_clan_id', 'destination': 'to_clan_id'})
    # Get the current date in YYYY-MM-DD format
    current_date = datetime.now().strftime("%Y-%m-%d")
    file_name = f"NWO_Rotations_{current_date}"

    # Function to convert DataFrame to Excel and return as a downloadable object
    def to_excel(df):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        processed_data = output.getvalue()
        return processed_data

    # Create an Excel file for download
    excel_data = to_excel(export_df)
    # Create a button for downloading the Excel file
    col1, col2 , _ , _= st.columns([1,1,1,1])
    with col1: 
        st.download_button(
            label="Download Excel file",
            data=excel_data,
            file_name=f"{file_name}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    count = len(export_df)

    st.write(f"{count} Movements / {len(st.session_state.players_df)} Ranked players")

  
