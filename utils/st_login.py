import streamlit as st
from typing import Optional
from time import sleep
import hmac


def check_password() -> bool :
    """
        Presents a login screen 
        set write autorisations in st.session_state["can_write"]
        Returns `True` if the user had a correct password 
    
    """

    def login_form():
        """Form with widgets to collect user information"""
        with st.form("Credentials"):
            st.selectbox("Username", key="username", options=st.secrets.passwords.keys(), index=None)
            st.text_input("Password", type="password", key="password")
            st.form_submit_button("Log in", on_click=password_entered)

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["username"] in st.secrets[ "passwords" ] and hmac.compare_digest(
            st.session_state["password"],
            st.secrets.passwords[st.session_state["username"]],
        ):
            st.session_state["password_correct"] = True
            st.session_state["can_write"] =  st.session_state["username"] in st.secrets[ "authorizations" ] and st.secrets.authorizations[ st.session_state["username"] ]  == "read_write"

            del st.session_state["password"]  # Don't store the username or password.
            del st.session_state["username"]

        else:
            st.session_state["password_correct"] = False

    # Return True if the username + password is validated.
    if st.session_state.get("password_correct", False):
        return True
    
    # Show inputs for username + password.
    login_form()
    if "password_correct" in st.session_state:
        st.error("😕 User not known or password incorrect")
    return False
