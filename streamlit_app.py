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

my_dataframe = session.table("smoothies.public.fruit_options").select(col("FRUIT_NAME"), col("SEARCH_ON"))

panda_df = my_dataframe.to_pandas()
st.dataframe(panda_df)
st.stop()

fruit_options = panda_df['FRUIT_NAME'].tolist()

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    options=fruit_options,
    max_selections=5
)
