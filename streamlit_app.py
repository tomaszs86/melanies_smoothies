import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import requests

# Snowpark session
connection_parameters = {
    "user": st.secrets["user"],
    "password": st.secrets["password"],
    "account": st.secrets["account"],
    "warehouse": st.secrets["warehouse"],
    "database": st.secrets["database"],
    "schema": st.secrets["schema"]
}
session = Session.builder.configs(connection_parameters).create()

fruit_df = session.table("smoothies.public.fruit_options").select(col("FRUIT_NAME"))
fruit_options = [row["FRUIT_NAME"] for row in fruit_df.collect()]

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    fruit_options,
    max_selections=5
)
