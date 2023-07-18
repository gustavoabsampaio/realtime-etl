-- CRIA A TABELA
DROP TABLE IF EXISTS weather_data;
CREATE TABLE weather_data (
    entry_num SERIAL PRIMARY KEY,
    station_id VARCHAR(4) NOT NULL,
    station_name VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT,
    measurement_timestamp TIMESTAMP,
    temperature FLOAT,
    rainfall FLOAT
);

-- CONSTRAINT PARA IMPEDIR DUPLICATAS
ALTER TABLE weather_data ADD CONSTRAINT
    unique_measurements_constraint UNIQUE (station_id, measurement_timestamp);

-- VIEWS

-- VIEW PARA A TEMPERATURA MEDIA DE CADA DIA
CREATE VIEW average_temperatures_daily AS
    SELECT station_name, to_char(AVG(temperature), '90.00') AS temp_avg, date(measurement_timestamp) as measurement_date
    FROM weather_data
    GROUP BY station_name, measurement_date
    ORDER BY measurement_date DESC;

-- VIEW PARA A TEMPERATURA MEDIA DAS ULTIMAS 24H
CREATE VIEW average_temperatures_24h AS
    SELECT station_name, to_char(AVG(temperature), '90.00') AS temp_avg
    FROM weather_data
    WHERE measurement_timestamp >= current_timestamp AT TIME ZONE 'SGT' - INTERVAL '1 day'
    GROUP BY station_name;

-- RETORNA A QUANTIDADE DE CHUVA TOTAL DIARIA
CREATE VIEW total_rain_daily AS
    SELECT station_name, to_char(SUM(rainfall), '990.00') total_rain, date(measurement_timestamp) as measurement_date
    FROM weather_data
    GROUP BY station_name, measurement_date
    ORDER BY measurement_date DESC;

CREATE VIEW total_rain_4h AS
    SELECT station_name, SUM(rainfall) AS total_rain
    FROM weather_data
    WHERE measurement_timestamp >= current_timestamp AT TIME ZONE 'SGT' - INTERVAL '4 hours'
    GROUP BY station_name;


-- PARA ADICIONAR A CONSTRAINT DE REMOVER DUPLICATAS FOI NECESSARIO REMOVER TODAS AS DUPLICATAS

-- RENOMEIA A TABELA ORIGINAL
ALTER TABLE weather_data RENAME TO weather_data_old;

-- TABELA NOVA PARA GUARDAR ENTRADAS UNICAS
CREATE TABLE weather_data AS SELECT * FROM weather_data_old LIMIT 0;

-- INSERE AS ENTRADAS UNICAS
INSERT INTO weather_data SELECT DISTINCT ON(station_id, measurement_timestamp) * FROM weather_data_old;

-- DROP NA TABELA ANTIGA
DROP TABLE weather_data_old;

-- ADICIONA O CONSTRAINT PARA EVITAR FUTURAS DUPLICATAS
ALTER TABLE weather_data ADD CONSTRAINT
    unique_measurements_constraint UNIQUE (station_id, measurement_timestamp);