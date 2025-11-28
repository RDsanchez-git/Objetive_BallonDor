import os
import glob
import pandas as pd
from sqlalchemy import create_engine

# --- 1. CONFIGURACIÓN ---
DB_CONFIG = {
    "host": os.environ.get('DB_HOST', 'localhost'),
    "port": os.environ.get('DB_PORT', '5432'),
    "name": os.environ.get('DB_NAME', 'meritocr_ai_db'),
    "user": os.environ.get('DB_USER', 'admin'),
    "pass": os.environ.get('DB_PASS', 'adminpass')
}
connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['pass']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['name']}"
engine = create_engine(connection_string)

DATA_DROP_PATH = "/app/data_drop"

def load_clean_file(csv_path):
    print(f"--> Cargando archivo procesado: {os.path.basename(csv_path)}")
    
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"❌ Error leyendo CSV: {e}")
        return

    # --- CORRECCIÓN CRÍTICA: MAPEO A SNAKE_CASE ---
    # Convertimos los nombres del CSV a nombres amigables con SQL (minúsculas)
    cols_map = {
        # Identificación
        'Player': 'player_name',
        'Nation': 'nation',
        'Pos_Primary': 'main_position_group',
        'Squad': 'squad',
        'Comp': 'league_id',
        'Age': 'age',
        'Born': 'born',
        'Season_ID': 'season_id',
        'Registro_Tipo': 'record_type', # (Total vs Parcial)
        
        # Tiempo de Juego
        'MP': 'matches_played',
        'Starts': 'starts',
        'Min': 'minutes_played',
        '90s': 'nineties',
        
        # Ofensiva
        'Gls': 'goals',
        'Ast': 'assists',
        'G+A': 'goals_assists',
        'PK': 'pk_goals',
        'PKatt': 'pk_attempts',
        
        # Avanzadas
        'xG': 'xg',
        'npxG': 'npxg',
        'xAG': 'xa',
        
        # Porteros (Si existen)
        'Saves': 'saves',
        'GA': 'goals_against',
        'SoTA': 'shots_on_target_against',
        'Save%': 'save_pct',
        'CS': 'clean_sheets'
    }
    
    # 1. Filtrar solo las columnas que existen en el CSV y están en nuestro mapa
    available_cols = [c for c in cols_map.keys() if c in df.columns]
    
    if not available_cols:
        print("⚠️ Error: No se encontraron columnas compatibles. Revisa el CSV.")
        return

    # 2. Renombrar las columnas al formato SQL
    df_db = df[available_cols].rename(columns=cols_map)
    
    # 3. Seguridad: Si falta season_id, poner 2023 por defecto
    if 'season_id' not in df_db.columns:
        df_db['season_id'] = 2023 

    # 4. Carga a SQL
    print(f"   -> Insertando {len(df_db)} registros en 'player_performance_real'...")
    try:
        # if_exists='replace' recreará la tabla con los nombres nuevos y correctos
        # O 'append' si ya la borraste manualmente. 'replace' es más seguro ahora.
        df_db.to_sql('player_performance_real', con=engine, if_exists='replace', index=False)
        print("   ✅ Carga exitosa.")
    except Exception as e:
        print(f"   ❌ Error de SQL: {e}")

def main():
    print(">>> INICIANDO LOADER V2 (FIX COLUMNAS SQL) <<<")
    
    # Buscamos archivos CLEAN
    files = glob.glob(f"{DATA_DROP_PATH}/*_clean.csv")
    
    if not files:
        print("⚠️  No se encontraron archivos *_clean.csv.")
        return

    for file in files:
        load_clean_file(file)
            
    print(">>> PROCESO COMPLETADO <<<")

if __name__ == "__main__":
    main()