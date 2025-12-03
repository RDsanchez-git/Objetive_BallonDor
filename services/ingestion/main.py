import os
import glob
import pandas as pd
from sqlalchemy import create_engine

# --- CONFIGURACIÓN ---
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
    print(f"--> Ingestando archivo Maestro V6: {os.path.basename(csv_path)}")
    
    try:
        df = pd.read_csv(csv_path)
        
        # Validación de Seguridad
        if 'player_name' not in df.columns:
            print("❌ Error: El archivo no parece tener el formato Elite (falta 'player_name').")
            return

        # Metadata extra si falta
        if 'season_id' not in df.columns:
            df['season_id'] = 2024-2025

        print(f"   -> Insertando {len(df)} registros con {len(df.columns)} columnas...")
        
        # CARGA DIRECTA (Porque el Notebook ya hizo todo el trabajo sucio)
        df.to_sql('player_performance_real', con=engine, if_exists='replace', index=False)
        print("   ✅ Carga exitosa.")
        
    except Exception as e:
        print(f"   ❌ Error cargando: {e}")

def main():
    print(">>> INICIANDO LOADER FINAL (V6) <<<")
    files = glob.glob(f"{DATA_DROP_PATH}/*_clean.csv")
    
    if not files:
        print("⚠️  No se encontraron archivos clean.")
        return

    for file in files:
        load_clean_file(file)

if __name__ == "__main__":
    main()