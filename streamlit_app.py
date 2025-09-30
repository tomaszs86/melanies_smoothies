import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import requests
import pandas as pd # Konieczne do użycia to_pandas() i loc

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

# 1. Połączenie Snowpark
connection_parameters = {
    "user": st.secrets["user"],
    "password": st.secrets["password"],
    "account": st.secrets["account"],
    "warehouse": st.secrets["warehouse"],
    "database": st.secrets["database"],
    "schema": st.secrets["schema"]
}
session = Session.builder.configs(connection_parameters).create()

# 2. Pobieranie danych i konwersja na Pandas
my_dataframe = session.table("smoothies.public.fruit_options").select(col("FRUIT_NAME"), col("SEARCH_ON"))
panda_df = my_dataframe.to_pandas()
# st.dataframe(panda_df)
# st.stop()

fruit_options = panda_df['FRUIT_NAME'].tolist()

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    options=fruit_options,
    max_selections=5
)


if ingredients_list:
    # Używamy tej linii do budowania stringa dla INSERT - jest poprawna
    ingredients_string = ' '.join(ingredients_list)

    # Logika dla wywołań API (musi być w pętli)
    for fruit_chosen in ingredients_list:
        
        # POPRAWKA 1: Użycie panda_df zamiast pd_df
        # POPRAWKA 2: Wcięcia są teraz poprawne (cała ta logika jest w pętli)
        search_on = panda_df.loc[panda_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        # st.write('The search value for ', fruit_chosen, ' is ', search_on, '.')
        
        st.subheader(fruit_chosen + ' Nutrition Information')
        
        # Wywołanie API
        # Pamiętaj, że URL musi być prawidłowy, a API musi zwracać JSON
        fruityvice_response = requests.get("https://my.smoothiefroot.com/api/fruit/" + search_on)
        fv_df = st.dataframe(data=fruityvice_response.json(), use_container_width=True)


    # 3. Finalizacja zamówienia (poza pętlą)
    
    # Tworzenie Snowpark DataFrame dla INSERT
    orders_df = session.create_dataframe(
        [[name_on_order, ingredients_string]],
        schema=["name_on_order", "ingredients"]        
    )

    st.write("Order preview:")
    st.write(orders_df.to_pandas())

    time_to_insert = st.button('Submit Order')
    if time_to_insert:
        # Wstawienie danych do tabeli
        # orders_df.write.insert_into("smoothies.public.orders")
        session.sql(f"""
        INSERT INTO smoothies.public.orders (name_on_order, ingredients)
        VALUES ('{name_on_order}', '{ingredients_string}')
""").collect()
        st.success('Your Smoothie is ordered!', icon="✅")
