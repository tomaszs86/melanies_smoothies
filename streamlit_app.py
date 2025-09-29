# Import python packages
import streamlit as st
import requests
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import pandas as pd

# -----------------------------------------------------
# FUNKCJA BUFORUJĄCA DANE (Rozwiązanie problemu timingowego)
# -----------------------------------------------------
@st.cache_data
def get_fruit_data(session):
    """Pobiera dane o owocach z bazy i konwertuje je na Pandas DataFrame
       dla szybkiego użycia w aplikacji."""
    try:
        snowpark_df = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'), col('SEARCH_ON'))
        return snowpark_df.to_pandas()
    except Exception as e:
        st.warning(f"Błąd: Nie można pobrać danych z tabeli FRUIT_OPTIONS. {e}")
        return pd.DataFrame()


# Write directly to the app
st.title(f" :cup_with_straw: Customize Your Smoothie! :cup_with_straw: ")
st.write(
    """Witaj w aplikacji Smoothie Ordering App!"""
)

# 1. Połączenie z sesją Snowpark
try:
    session = get_active_session()
except Exception as e:
    st.error(f"Błąd inicjalizacji sesji Snowpark. Proszę uruchomić aplikację w Snowsight. Szczegóły: {e}")
    st.stop()


# 2. Pobieranie danych z bazy za pomocą Snowpark i cache
fruit_data_for_app = get_fruit_data(session)

if fruit_data_for_app.empty:
    st.warning("Nie można załadować opcji owoców. Upewnij się, że tabela FRUIT_OPTIONS istnieje i masz uprawnienia.")
    st.stop()


# Tworzenie mapy i listy opcji (oparte o Pandas DATAFRAME)
fruit_options = fruit_data_for_app['FRUIT_NAME'].tolist()
fruit_search_map = fruit_data_for_app.set_index('FRUIT_NAME')['SEARCH_ON'].to_dict()


# 3. Multiselect
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    fruit_options,
    max_selections=5
)

# ------------------
# Sekcja logiki biznesowej i wywołań API
# ------------------

if ingredients_list:
    ingredients_string = ''
    
    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '
        
        # 1. Określenie terminu wyszukiwania z mapy (zachowanie logiki biznesowej)
        search_term = fruit_search_map.get(fruit_chosen, fruit_chosen)
        
        # 2. Wywołanie API
        st.subheader(fruit_chosen + ' Nutrition Information')
        smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/" + search_term)
        
        # 3. Wyświetlenie danych z API
        st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)

    
    # Budowa instrukcji INSERT
    name_on_order = st.text_input("Name on Smoothie:")
    st.write("The name on your Smoothie will be: ", name_on_order)
    
    my_insert_stmt = f"""
        INSERT INTO smoothies.public.orders(ingredients, name_on_order)
        VALUES ('{ingredients_string}', '{name_on_order}')
    """

    st.write(my_insert_stmt)

    time_to_insert = st.button('Submit Order')

    if time_to_insert:
        # Wykonanie INSERT za pomocą Snowpark
        session.sql(my_insert_stmt).collect()
        st.success('Your Smoothie is ordered!', icon="✅")
