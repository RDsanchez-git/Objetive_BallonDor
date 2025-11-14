import os
import time
import pandas as pd
from sqlalchemy import create_engine

# --- 1. CONEXIÓN A LA BASE DE DATOS ---
# Docker-compose pasa estas variables de entorno
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')

try:
    # Creamos el "motor" de conexión usando SQLAlchemy
    connection_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)
    print("Conexión a PostgreSQL exitosa.")
except Exception as e:
    print(f"Error al conectar a la DB: {e}")
    # Si no podemos conectarnos, el servicio debe fallar
    exit(1)


# --- 2. FUNCIONES DE SCRAPING (Multi-Fuente) ---

def scrape_fbref(season_year):
    """
    Simulación de scraping a FBref para métricas avanzadas.
    Implementa tu lógica de scraping aquí (con requests/BS4).
    ¡Recuerda manejar el Rate Limiting!
    """
    print(f"Scrapeando FBref para la temporada {season_year}...")
    time.sleep(5) # ¡IMPORTANTE! Respetar el rate limiting
    # ... Lógica de scraping ...
    # Devuelve un DataFrame de Pandas normalizado
    data = {
        'player_name': ['Player A', 'Player B'],
        'season_name': [f"{season_year}/{season_year+1}", f"{season_year}/{season_year+1}"],
        'goals': [10, 20],
        'xg': [8.5, 22.1]
        # ... más métricas
    }
    df = pd.DataFrame(data)
    df['data_source'] = 'FBref'
    return df

def scrape_transfermarkt(season_year):
    """
    Simulación de scraping a Transfermarkt para valores de mercado (proxy de sesgo).
    """
    print(f"Scrapeando Transfermarkt para la temporada {season_year}...")
    time.sleep(5)
    # ... Lógica de scraping ...
    data = {
        'player_name': ['Player A', 'Player B'],
        'market_value_eur': [50000000, 120000000]
    }
    df = pd.DataFrame(data)
    df['data_source'] = 'Transfermarkt'
    return df

# --- 3. ORQUESTACIÓN DE INGESTA (main) ---

def main():
    print("Iniciando el servicio de ingesta...")
    
    seasons_to_scrape = range(2000, 2025) # 2000-Presente
    
    all_raw_data = []

    for year in seasons_to_scrape:
        try:
            # Adquisición Multi-Fuente
            df_fbref = scrape_fbref(year)
            df_tm = scrape_transfermarkt(year)
            
            # Normalización de Schemas y Join
            # (Aquí normalizas nombres de jugadores, IDs, etc., antes de hacer merge)
            # df_season = pd.merge(df_fbref, df_tm, on='player_name', how='outer')
            
            # (Simplificación: solo usamos FBref por ahora)
            all_raw_data.append(df_fbref)

        except Exception as e:
            print(f"Error al procesar la temporada {year}: {e}")

    if not all_raw_data:
        print("No se obtuvieron datos. Terminando.")
        return

    # Combinamos todos los DataFrames
    final_df = pd.concat(all_raw_data, ignore_index=True)

    # --- 4. CARGA A POSTGRESQL (SSOT) ---
    try:
        print(f"Cargando {len(final_df)} registros a la tabla 'player_performance_raw'...")
        
        # (Aquí faltaría el pre-procesamiento para obtener/crear IDs foráneos)
        
        # Cargamos los datos crudos a la tabla
        # final_df.to_sql(
        #     'player_performance_raw', 
        #     con=engine, 
        #     if_exists='append', # Añade los datos, no sobrescribe
        #     index=False
        # )
        
        print("Carga de datos crudos completada.")
        
    except Exception as e:
        print(f"Error al cargar datos en la DB: {e}")

    print("Servicio de ingesta finalizado.")


if __name__ == "__main__":
    main()