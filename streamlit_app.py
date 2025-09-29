# Import python packages
import streamlit as st
import requests
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import pandas as pd

# Write directly to the app
st.title("Customize Your Smoothie!")
st.write(
    """Witaj w aplikacji Smoothie Ordering App!"""
)

# 1. Połączenie z sesją Snowpark
# Ta funkcja jest gwarantowana, że działa w SiS.
try:
    session = get_active_session()
except Exception as e:
    st.error(f"Błąd inicjalizacji sesji Snowpark. Proszę uruchomić aplikację w Snowsight. Szczegóły: {e}")
    st.stop()


name_on_order = st.text_input("Name on Smoothie:")
st.write("The name on your Smoothie will be: ", name_on_order)

# 2. Pobieranie danych z bazy za pomocą Snowpark (pobieramy obie kolumny)
try:
    # Wczytujemy dane do Dataframe Snowpark
    my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'), col('SEARCH_ON'))
except Exception as e:
    st.warning("Nie można załadować tabeli FRUIT_OPTIONS. Sprawdź kontekst roli i uprawnienia.")
    st.stop()


# 3. PRZECHWYCENIE DANYCH DO PANDAS (Operacja kończąca musi być wykonana tylko raz)
# Konwersja Snowpark DataFrame do Pandas DataFrame jest konieczna dla st.multiselect
try:
    fruit_data_for_app = my_dataframe.to_pandas()
    fruit_options = fruit_data_for_app['FRUIT_NAME'].tolist()
except Exception as e:
    st.warning("Błąd konwersji danych na listę opcji.")
    st.stop()


# 4. Multiselect (wyświetla FRUIT_NAME)
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
    
    # 5. Iteracja i budowanie Stringa + Wywołania API
    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '
        
        # Logika biznesowa: filtrujemy lokalny Pandas DataFrame, który jest już załadowany.
        # Jest to szybsze i bardziej niezawodne niż ponowne użycie Snowpark DF.
        search_term_row = fruit_data_for_app[fruit_data_for_app['FRUIT_NAME'] == fruit_chosen]
        
        if not search_term_row.empty:
            # Używamy SEARCH_ON do wywołania API
            search_term = search_term_row['SEARCH_ON'].iloc[0]
            
            st.subheader(fruit_chosen + ' Nutrition Information')
            
            # Wywołanie API (wewnątrz pętli)
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
