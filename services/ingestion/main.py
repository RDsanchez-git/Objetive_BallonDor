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
    print(f"--> Ingestando archivo ya normalizado: {os.path.basename(csv_path)}")
    
    try:
        df = pd.read_csv(csv_path)
        
        # VALIDACIÓN: Verificar si las columnas ya están en formato DB (minúsculas)
        # Si el usuario corrió el Notebook 00 correctamente, 'Player' ya debería ser 'player_name'
        if 'player_name' not in df.columns and 'Player' in df.columns:
            print("⚠️ ADVERTENCIA: El archivo parece no tener los nombres estandarizados.")
            print("   Por favor, ejecuta el Notebook 00 actualizado.")
            return

        # Asegurar season_id
        if 'season_id' not in df.columns:
            df['season_id'] = 2024

        # CARGA DIRECTA
        print(f"   -> Insertando {len(df)} registros...")
        # 'replace' es seguro aquí porque queremos que la tabla sea idéntica al CSV limpio
        df.to_sql('player_performance_real', con=engine, if_exists='replace', index=False)
        print("   ✅ Carga exitosa.")
        
    except Exception as e:
        print(f"   ❌ Error cargando {csv_path}: {e}")

def main():
    print(">>> INICIANDO LOADER V4 (DIRECT LOAD) <<<")
    
    files = glob.glob(f"{DATA_DROP_PATH}/*_clean.csv")
    
    if not files:
        print("⚠️  No se encontraron archivos clean.")
        return

    for file in files:
        load_clean_file(file)

if __name__ == "__main__":
    main()