import base64
import pandas as pd

import requests
from io import StringIO



# -----------------------------------------------------------------------------
# Base 64 decode

def base64_padding(source_str : str) -> str: 
    """ Base 64 has = or == endings in standard and our strings don't have them, hence we need a conversion"""
    pad = {0: "", 2: "==", 3: "="}

    mod4 = (len(source_str)) % 4
    if (mod4 == 1):
        raise ValueError("Invalid input: its length mod 4 == 1")
    b64u = source_str + pad[mod4]
    padded_str = base64.b64decode(b64u)
    return padded_str

    # example to b64decode a link clan part 
    # linksEnd = "eyJ1aWQiOiIxNDA0MDkifQ", 
    # print(json.loads(base64_padding(linkEnd).decode("utf8"))["uid"])



def get_last_day_link(clan_id : int ) -> str:
    """ Gets Url for last reset clan report 
        
        inputs:
             clan_id 
    """

    def get_clan_string(clan_id : int ) -> str :
        return f'{{"uid":"{str(clan_id)}"}}'
    
    def b64_url_encode(source_string : str ) -> str :
        """ base 64 encoded of string without trailing = """
        return base64.b64encode(source_string.encode("utf8")).decode("utf8").rstrip('=')
        
    link_template = "https://www.armyneedyou.com/team/user_export?type=current&dateType=lastday&token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9."
    clan_string = get_clan_string(clan_id)
    clan_string_encoded = b64_url_encode(clan_string)
    url = f"{link_template}{clan_string_encoded}.NWOTOKEN&battle=pvp"

    print(url)
    return url



 

def pull_all_aow_links(clan_tag : str , 
                   clan_ids :dict[str: dict] ,
                   clan_names : list[str]  )  -> pd.DataFrame :

    print(f" Pull links for {clan_tag}")
    target_df=  pd.DataFrame() 

    for clan_id in clan_ids [clan_tag]: 
        url = get_last_day_link(clan_id)
        response = requests.get(url)
        csv_data = response.content.decode('utf-8')
        csv_file = StringIO(csv_data)
        local_df = pd.read_csv(csv_file)
        local_df = local_df.iloc[:-1]
        local_df['Team'] = clan_tag    # NWO , BRA , SH or RES
        local_df['Current Clan'] = clan_id
        local_df['Current Clan Name'] = clan_names[clan_id] if clan_id in clan_names else ""

        local_df['ID'] = local_df['ID'].astype(int)
        target_df = pd.concat( [target_df,local_df], ignore_index=True)
        
    return target_df


