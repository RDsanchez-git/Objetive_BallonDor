import os
import time
import random
import pandas as pd
import requests # Necesario para el anti-bloqueo
from sqlalchemy import create_engine
import soccerdata as sd

# =============================================================================
# 🛡️ ANTI-BLOQUEO (MONKEY PATCH)
# Esto obliga a soccerdata a usar una identificación de navegador real
# =============================================================================
original_request = requests.Session.request

def patched_request(self, method, url, *args, **kwargs):
    # Definimos headers de un navegador real
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://fbref.com/'
    }
    # Si ya hay headers, los actualizamos; si no, los creamos
    kwargs.setdefault('headers', {})
    kwargs['headers'].update(headers)
    
    # Ejecutamos la petición original pero con nuestro "disfraz"
    return original_request(self, method, url, *args, **kwargs)

# Aplicamos el parche a la librería requests (que usa soccerdata por debajo)
requests.Session.request = patched_request
# =============================================================================


# --- 1. CONFIGURACIÓN DE ENTORNO ---
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME', 'meritocr_ai_db')
DB_USER = os.environ.get('DB_USER', 'admin')
DB_PASS = os.environ.get('DB_PASS', 'adminpass')

connection_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

# --- 2. PARÁMETROS DEL SCRAPER ---
LEAGUES = ['ENG-Premier League', 'ESP-La Liga', 'ITA-Serie A', 'GER-Bundesliga', 'FRA-Ligue 1']
YEARS = ['2023'] # Solo 2023 para probar

def flatten_columns(df):
    if isinstance(df.columns, pd.MultiIndex):
        new_cols = []
        for col in df.columns.values:
            if col[0]: 
                new_cols.append(f"{col[0]}_{col[1]}")
            else:
                new_cols.append(col[1])
        df.columns = new_cols
    return df

def process_season(scraper, league, year):
    print(f"--> Procesando: {league} | Temporada: {year}")
    
    try:
        # A. STANDARD
        df_std = scraper.read_player_season_stats(stat_type='standard')
        df_std = df_std.reset_index()
        df_std = flatten_columns(df_std)
        
        cols_std_map = {
            'player': 'Player', 'nation': 'Nation', 'pos': 'Pos', 'team': 'Squad', 'age': 'Age', 'born': 'Born',
            'Playing Time_MP': 'MP', 'Playing Time_Starts': 'Starts', 'Playing Time_Min': 'Min',
            'Performance_Gls': 'Gls', 'Performance_Ast': 'Ast', 'Performance_G+A': 'G_A',
            'Performance_PK': 'PK', 'Performance_PKatt': 'PKatt',
            'Performance_CrdY': 'CrdY', 'Performance_CrdR': 'CrdR'
        }
        exist_cols = [c for c in cols_std_map.keys() if c in df_std.columns]
        df_std = df_std[exist_cols].rename(columns=cols_std_map)
        
        time.sleep(random.uniform(3, 5)) # Pausa un poco más larga

        # B. PORTEROS
        try:
            df_gk = scraper.read_player_season_stats(stat_type='keepers')
            df_gk = df_gk.reset_index()
            df_gk = flatten_columns(df_gk)
            
            cols_gk_map = {
                'player': 'Player', 'team': 'Squad',
                'Performance_GA': 'GA', 'Performance_Ga90': 'Ga90',
                'Performance_SoTA': 'SoTA', 'Performance_Saves': 'Saves',
                'Performance_Save%': 'Save_Pct', 'Performance_CS': 'CS', 'Performance_CS%': 'CS_Pct'
            }
            exist_cols_gk = [c for c in cols_gk_map.keys() if c in df_gk.columns]
            df_gk = df_gk[exist_cols_gk].rename(columns=cols_gk_map)
            
            df_combined = pd.merge(df_std, df_gk, on=['Player', 'Squad'], how='left')
        except Exception as e:
            print(f"   [!] Aviso: Saltando porteros: {e}")
            df_combined = df_std

        time.sleep(random.uniform(3, 5))

        # C. CONTEXTO EQUIPO
        try:
            df_table = scraper.read_league_table()
            df_table = df_table.reset_index()
            cols_table_map = {'team': 'Squad', 'Rk': 'League_Rank', 'GF': 'Squad_GF', 'GA': 'Squad_GA', 'Pts': 'Squad_Pts'}
            exist_cols_table = [c for c in cols_table_map.keys() if c in df_table.columns]
            df_table = df_table[exist_cols_table].rename(columns=cols_table_map)
            
            df_final = pd.merge(df_combined, df_table, on='Squad', how='left')
        except Exception as e:
            print(f"   [!] Aviso: Saltando tabla posiciones: {e}")
            df_final = df_combined

        # CARGA
        df_final['Season_ID'] = int(year)
        df_final['League_ID'] = league
        df_final['Min'] = df_final['Min'].fillna(0)
        
        print(f"   -> Guardando {len(df_final)} registros...")
        df_final.to_sql('player_performance_real', con=engine, if_exists='append', index=False)
        return True

    except Exception as e:
        print(f"!!! ERROR CRÍTICO en {league}: {e}")
        return False

def main():
    print(">>> INICIANDO SCRAPER CON MÁSCARA DE NAVEGADOR <<<")
    # Intentamos limpiar caché corrupta de soccerdata si existe
    try:
        import shutil
        cache_path = os.path.expanduser("~/soccerdata")
        if os.path.exists(cache_path):
            print("   Limpiando caché vieja de soccerdata...")
            shutil.rmtree(cache_path)
    except:
        pass

    for year in YEARS:
        # no_cache=True para forzar el uso de nuestros nuevos headers
        scraper = sd.FBref(leagues=LEAGUES, seasons=[year], no_cache=True)
        for league in LEAGUES:
            process_season(scraper, league, year)
            time.sleep(random.uniform(5, 8))
            
    print(">>> PROCESO COMPLETADO <<<")

if __name__ == "__main__":
    main()