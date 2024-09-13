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


# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title="NWO transfers",
    page_icon=":recycle:",  # This is an emoji shortcode. Could be a URL too.
)
link_temmplate = "https://www.armyneedyou.com/team/user_export?type=current&dateType=lastday&token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9."
clan_ids =  { 
    "NWO"	: [  835 ],
    "RES" :	[ 
            47257, 
            10283 , 
            115,
            4023,
            120420,
            3037
            ],
    "BRA" : [ 
        140409,
        8961,
        96873,
        103475,
        111,
        434,
        4200
    ],
    "SH" : [ 
        5425,
        143430,
        133909
    ]
}
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
    
possible_clans = list(clan_ids.keys())
possible_clans  = possible_clans[1:] # all teams but NWO
 
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

if False:
    # decode 
    linksEnds = { "BRA" : 
                ["eyJ1aWQiOiIxNDA0MDkifQ", "eyJ1aWQiOiI4OTYxIn0","eyJ1aWQiOiI5Njg3MyJ9","eyJ1aWQiOiIxMDM0NzUifQ","eyJ1aWQiOiIxMTEifQ","eyJ1aWQiOiI0MzQifQ","eyJ1aWQiOiI0MjAwIn0"
                    
                ],
                "SH": [
                    "eyJ1aWQiOiI1NDI1In0", "eyJ1aWQiOiIxNDM0MzAifQ", "eyJ1aWQiOiIxMzM5MDkifQ"
                ]
                }
    for team in linksEnds :
        print(f'"{team}" : [')
        for linkEnd in linksEnds[team]:
            print(json.loads(base64_padding(linkEnd).decode("utf8"))["uid"])
            print(',')
        print(f']')

def base64_padding(source_str): 
    pad = {0: "", 2: "==", 3: "="}

    mod4 = (len(source_str)) % 4
    if (mod4 == 1):
        raise ValueError("Invalid input: its length mod 4 == 1")
    b64u = source_str + pad[mod4]
    padded_str = base64.b64decode(b64u)
    return padded_str


# -----------------------------------------------------------------------------
# Declare some useful functions.


def connect_db():
    """Connects to the sqlite database."""

    DB_FILENAME = Path(__file__).parent / "players.db"
    db_already_exists = DB_FILENAME.exists()

    conn = sqlite3.connect(DB_FILENAME)
    db_was_just_created = not db_already_exists

    return conn, db_was_just_created


def initialize_data(conn):
    """Initializes the players table with some data."""
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

    cursor.execute(
        """
        INSERT INTO players
            (player_id , player_name,  clan)
        VALUES
            -- RES
            (3053783 , 'Toto',  'RES'),
    
            -- SH
            (13567860 , 'Sandokan',  'SH'),

            -- BRA
            (7458701 ,'Ceara',  'SH')

        """
    )
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


def load_data(conn):
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


def update_data(conn, df, changes):
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

            if row_dict["clan"]  is None or row_dict["clan"] in possible_clans :
                rows.append(row_dict)
            else :
                st.toast(f"Invalid clan of origin `{row_dict['clan']}` for player {row_dict['player_name']}. Value must be in {str(possible_clans)}")


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

This page saves clan of origin for each player in our clan database.
This page then proposes you to doanload last available reset rank and calculate all clan moves. 
Top players will be in NWO and other players will be sorted in their own clan of origin.
"""

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

# Load data from database
df = load_data(conn)
df = df.sort_values(by='clan', ascending=True)
# Display data with editable table
edited_df = st.data_editor(
    df,
    num_rows="dynamic",  # Allow appending/deleting rows.
    column_config={
        # Show dollar sign before price columns.
        #"price": st.column_config.NumberColumn(format="$%.2f"),
        #"cost_price": st.column_config.NumberColumn(format="$%.2f"),
    },
    key="player_table",
)

has_uncommitted_changes = any(len(v) for v in st.session_state.player_table.values())

st.button(
    ":warning: Save Changes",
    type="primary",
    disabled=not has_uncommitted_changes,
    # Update data in database
    on_click=update_data,
    args=(conn, df, st.session_state.player_table),
)
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
    url = f"{link_temmplate}{clan_string_encoded}.NWOTOKEN&battle=pvp"
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

    matching_row = df[df['player_id'] - row['ID'] == 0]
    clan = matching_row['clan'].values[0] if not matching_row.empty else None
    if clan and len(clan) > 0:
        return clan
    else :
        print(f"not found {row['ID']}  {row['Current Clan Name']} ")
        #print(matching_row)
    return np.random.choice(possible_clans)
    

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

    ## All players

    st.session_state.players_df = st.data_editor(
        st.session_state.players_df,
        num_rows="dynamic")
    
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
            "dest team", "rank"]
            )
    for index, nwo_clan_id in enumerate(clan_ids["NWO"]):
        for _, row in st.session_state.players_df.iloc[50*index:50+50*index].iterrows():
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

    remaining_players_df =  st.session_state.players_df.iloc[50*nwo_clan_count:]
    
    clans_to_sort = possible_clans
    for clan_to_sort in clans_to_sort: 
        print(f"clan to sort: {clan_to_sort}")
        print()
        remaining_players_clan = remaining_players_df[remaining_players_df['origin'] == clan_to_sort]
        for index, target_clan_id in enumerate(clan_ids[clan_to_sort]):
            for _, row in remaining_players_clan[50*index:50+50*index].iterrows():
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
    make sure that clans are full but not over 50 
    """)
    edited_movesdf = st.data_editor(
        st.session_state.movesdf,
        num_rows="dynamic",
        disabled=("player_id",
            "player_name",
            "origin",
            "current team","rank"),
        column_order = ("player_id",
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
    with col1:
        ldf = edited_movesdf[edited_movesdf['dest team'] == "NWO"]["dest team"].value_counts()
        st.dataframe(ldf)

    with col2:
        ldf = edited_movesdf[edited_movesdf['dest team'].str.contains('BRA', case=False, na=False)]["dest team"].value_counts()
        st.dataframe(ldf)
    with col3:
        ldf = edited_movesdf[edited_movesdf['dest team'].str.contains('RES', case=False, na=False)]["dest team"].value_counts()
        st.dataframe(ldf)
    with col4:
        ldf = edited_movesdf[edited_movesdf['dest team'].str.contains('SH', case=False, na=False)]["dest team"].value_counts()
        st.dataframe(ldf)


    st.subheader("Moves formated")
    st.info(
    """
    Use the following text to send to the devs
    """
    )   

    move_list="""
a) Rom Ⓡᵉˢ, id 2770772, NWO GENERAL
b)"""

    for key, value in clan_names.items():
            move_list = f"{move_list} {value},"
    move_list=move_list.rstrip(",")

    move_list =f"""{move_list}
c)"""
    sorted_move_list = edited_movesdf.sort_values(by="from")
    for _, row in sorted_move_list.iterrows():
        if not pd.isna(row["destination"] )  and  row["destination"]  !=  row["from"]:
            move_list = f"{move_list}\n{row["player_id"]} - {row["from"]:.0f} - {row["destination"]:.0f}"
    st.code(move_list)



