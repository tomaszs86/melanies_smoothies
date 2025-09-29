# Import python packages
import streamlit as st
import snowflake.connector
import requests

# Write directly to the app
st.title(f" :cup_with_straw: Example Streamlit App :cup_with_straw: {st.__version__}")
st.write(
  """Replace this example with your own code!
  **And if you're new to Streamlit,** check
  out our easy-to-follow guides at
  [docs.streamlit.io](https://docs.streamlit.io).
  """
)

name_on_order = st.text_input("Name on Smoothie:")
st.write("The name on your Smoothie will be: ", name_on_order)

# Connect to Snowflake (using secrets.toml)
conn = snowflake.connector.connect(
    user=st.secrets["user"],
    password=st.secrets["password"],
    account=st.secrets["account"],
    warehouse=st.secrets["warehouse"],
    database=st.secrets["database"],
    schema=st.secrets["schema"]
)
cur = conn.cursor()

# Get fruit options
cur.execute("SELECT FRUIT_NAME FROM smoothies.public.fruit_options")
fruit_options = [row[0] for row in cur.fetchall()]

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    fruit_options,
    max_selections=5
)

smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/watermelon")
# st.text(smoothiefroot_response.json())
sf_df = st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)

if ingredients_list:
    ingredients_string = " ".join(ingredients_list)

    my_insert_stmt = f"""
        INSERT INTO smoothies.public.orders(ingredients, name_on_order)
        VALUES ('{ingredients_string}', '{name_on_order}')
    """

    st.write(my_insert_stmt)

    time_to_insert = st.button('Submit Order')

    if time_to_insert:
        cur.execute(my_insert_stmt)
        conn.commit()
        st.success('Your Smoothie is ordered!', icon="✅")
