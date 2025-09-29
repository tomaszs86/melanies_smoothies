# Import python packages
import streamlit as st
import requests
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import pandas as pd

# Write directly to the app
st.title(f" :cup_with_straw: Example Streamlit App :cup_with_straw: {st.__version__}")
st.write(
    """Witaj w aplikacji Smoothie Ordering App!"""
    """Proszę uruchom tę aplikację w Snowsight (Streamlit-in-Snowflake)"""
)

# 1. Połączenie z sesją Snowpark
try:
    session = get_active_session()
except Exception as e:
    st.error(f"Błąd połączenia Snowpark. Aplikacja musi być uruchomiona w środowisku Snowflake. Szczegóły: {e}")
    st.stop() # Zatrzymanie aplikacji, jeśli sesja nie może być utworzona


name_on_order = st.text_input("Name on Smoothie:")
st.write("The name on your Smoothie will be: ", name_on_order)

# 2. Pobieranie danych z bazy za pomocą Snowpark (pobieramy obie kolumny)
try:
    my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'), col('SEARCH_ON'))
except Exception as e:
    st.warning("Nie znaleziono tabeli FRUIT_OPTIONS. Upewnij się, że używasz poprawnego kontekstu roli.")
    st.stop()


# Konwersja DataFrame Snowpark na listę dla multiselect (tylko FRUIT_NAME do wyświetlenia)
fruit_options = my_dataframe.select('FRUIT_NAME').to_pandas()['FRUIT_NAME'].tolist()

# 3. Multiselect (wyświetla FRUIT_NAME)
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
        
        # Logika biznesowa: dla każdego owocu pobierz odpowiedni SEARCH_ON
        search_term_df = my_dataframe.filter(col('FRUIT_NAME') == fruit_chosen).collect()
        
        if search_term_df:
            # Używamy SEARCH_ON do wywołania API
            search_term = search_term_df[0]['SEARCH_ON']
            
            st.subheader(fruit_chosen + ' Nutrition Information')
            # Wywołanie API następuje wewnątrz pętli, dla każdego wybranego owocu
            smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/" + search_term)
            st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)

    
    # Budowa instrukcji INSERT
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
