-- Crear tablas para la estructura de datos

CREATE TABLE seasons (
    season_id SERIAL PRIMARY KEY,
    season_name VARCHAR(10) NOT NULL UNIQUE -- ej. "2000/01"
);

CREATE TABLE players (
    player_id VARCHAR(50) PRIMARY KEY, -- ej. "messi_lionel" o ID de FBref
    player_name VARCHAR(255) NOT NULL,
    main_position_group VARCHAR(50) -- ej. 'Forward', 'Midfielder', 'Defender'
);

CREATE TABLE teams (
    team_id VARCHAR(50) PRIMARY KEY,
    team_name VARCHAR(255) NOT NULL
);

-- Tabla principal de rendimiento (Datos Crudos)
CREATE TABLE player_performance_raw (
    id SERIAL PRIMARY KEY,
    player_id VARCHAR(50) REFERENCES players(player_id),
    season_id INT REFERENCES seasons(season_id),
    team_id VARCHAR(50) REFERENCES teams(team_id),
    
    -- Métricas universales (disponibles 2000-presente)
    matches_played INT,
    minutes_played INT,
    goals INT,
    assists INT,
    
    -- Métricas avanzadas (NULL para período pre-xG)
    xg NUMERIC(5, 2), -- Goles Esperados
    xa NUMERIC(5, 2), -- Asistencias Esperadas
    progressive_passes INT,
    defensive_duels_won INT,

    -- Métricas de contexto (Sesgo)
    market_value_eur BIGINT, -- de Transfermarkt
    
    -- Fuente de datos (para trazabilidad)
    data_source VARCHAR(50), -- ej. 'FBref', 'Transfermarkt'

    CONSTRAINT unique_player_season UNIQUE (player_id, season_id, data_source)
);

-- Tabla para los candidatos y el resultado histórico
CREATE TABLE ballon_dor_candidates (
    id SERIAL PRIMARY KEY,
    season_id INT REFERENCES seasons(season_id),
    player_id VARCHAR(50) REFERENCES players(player_id),
    historical_rank INT, -- El ranking real de la votación
    
    -- Esta es la etiqueta que crearás en Fase 2 (Ingeniería de Features)
    ground_truth_relevance_score NUMERIC(5, 2) -- Tu puntaje 0-100
);

-- Puedes añadir más tablas para éxito colectivo (team_achievements)