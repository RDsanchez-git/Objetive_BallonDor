import os
import time
import pandas as pd
from sqlalchemy import create_engine, text

# --- 1. CONEXIÓN A LA BASE DE DATOS ---
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')

try:
    connection_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)
    print("Conexión a PostgreSQL exitosa.")
except Exception as e:
    print(f"Error al conectar a la DB: {e}")
    exit(1)

# --- 2. PREPARACIÓN DE DATOS MAESTROS (Foreign Keys) ---
def ensure_master_data_exists(season_year):
    """
    Inserta datos falsos en players, teams y seasons para satisfacer las Foreign Keys.
    En producción, esto se llenaría con datos reales.
    """
    with engine.connect() as conn:
        # 1. Crear Temporada
        season_id = season_year
        season_name = f"{season_year}/{season_year+1}"
        conn.execute(text(f"""
            INSERT INTO seasons (season_id, season_name) 
            VALUES ({season_id}, '{season_name}') 
            ON CONFLICT (season_id) DO NOTHING;
        """))

        # 2. Crear Jugadores (Player A y Player B)
        # Usamos IDs simples como 'player_a' para probar
        conn.execute(text("""
            INSERT INTO players (player_id, player_name, main_position_group) 
            VALUES 
                ('player_a', 'Player A', 'Forward'),
                ('player_b', 'Player B', 'Midfielder')
            ON CONFLICT (player_id) DO NOTHING;
        """))

        # 3. Crear Equipo (Team Test)
        conn.execute(text("""
            INSERT INTO teams (team_id, team_name) 
            VALUES ('team_test', 'Team Test FC')
            ON CONFLICT (team_id) DO NOTHING;
        """))
        
        conn.commit()

# --- 3. FUNCIONES DE SCRAPING ---
def scrape_fbref(season_year):
    print(f"Scrapeando FBref para la temporada {season_year}...")
    # time.sleep(1) # Reducido para que sea rápido en la prueba
    
    # IMPORTANTE: Ahora incluimos los IDs que coinciden con los datos maestros de arriba
    data = {
        'player_id': ['player_a', 'player_b'],
        'season_id': [season_year, season_year],
        'team_id': ['team_test', 'team_test'],
        'goals': [10, 20],
        'xg': [8.5, 22.1],
        'matches_played': [30, 28],
        'data_source': ['FBref', 'FBref']
    }
    return pd.DataFrame(data)

# --- 4. ORQUESTACIÓN (MAIN) ---
def main():
    print("Iniciando el servicio de ingesta...")
    
    # Probemos solo 3 años para que sea rápido
    seasons_to_scrape = range(2020, 2023) 
    
    all_raw_data = []

    for year in seasons_to_scrape:
        try:
            # 1. Asegurar que existen las referencias (FKs)
            ensure_master_data_exists(year)
            
            # 2. Obtener datos
            df_fbref = scrape_fbref(year)
            all_raw_data.append(df_fbref)

        except Exception as e:
            print(f"Error al procesar la temporada {year}: {e}")

    if not all_raw_data:
        print("No se obtuvieron datos.")
        return

    final_df = pd.concat(all_raw_data, ignore_index=True)

    # --- 5. CARGA A POSTGRESQL ---
    try:
        print(f"Cargando {len(final_df)} registros a la tabla 'player_performance_raw'...")
        
        # ¡AQUÍ ESTÁ LA CLAVE! Descomentado y funcionando
        final_df.to_sql(
            'player_performance_raw', 
            con=engine, 
            if_exists='append', # Agrega los datos
            index=False
        )
        
        print("¡Carga de datos crudos completada con éxito!")
        
    except Exception as e:
        print(f"Error CRÍTICO al cargar datos en la DB: {e}")

if __name__ == "__main__":
    main()