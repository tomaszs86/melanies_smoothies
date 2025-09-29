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

name_on_order = st.text_input("Your name on the order:")

if ingredients_list:
    for fruit_chosen in ingredients_list:
        st.subheader(f"{fruit_chosen} Nutrition Information")
        smoothiefroot_response = requests.get(f"https://my.smoothiefroot.com/api/fruit/{fruit_chosen}")
        sf_df = st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)

    ingredients_string = ' '.join(ingredients_list)
    orders_df = session.create_dataframe(
        [[ingredients_string, name_on_order]],
        schema=["ingredients", "name_on_order"]
    )

    st.write("Order preview:")
    st.write(orders_df.to_pandas())

    time_to_insert = st.button('Submit Order')
    if time_to_insert:
        orders_df.write.mode("append").save_as_table("smoothies.public.orders")
        st.success('Your Smoothie is ordered!', icon="✅")
