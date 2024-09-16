# 🛍️ NWO transfer 

A Streamlit app showing how to use `st.data_editor` to read and modify a database. Behind the scenes
this uses a simple SQLite database, but you can easily replace it with whatever your favorite DB is.

[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://inventory-tracker-template.streamlit.app/)

### How to run it on your own machine

1. Install the requirements

   ```bash
   $ pip install -r requirements.txt
   ```


2. create a pg sql db locally or in the cloud

   - create an account on https://cloud.tembo.io/
   - create a standard free tier postrgres instance
   - wait for the instance to load
   - press "show connection strings" 
   - copy hostname, port, username and password in .streamlit/secrets.toml

   ```toml
   "DB_HOST" = "<hostname>"
   "DB_USER" = "<user>"
   "DB_PASS" = "<password>"
   "DB_PORT" = <port>
   ``` 

3. Run the app

   ```bash
   $ streamlit run streamlit_app.py
   ```
