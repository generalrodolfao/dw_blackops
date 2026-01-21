-- Dim Airports
CREATE TABLE IF NOT EXISTS mart.dim_airport (
    airport_sk SERIAL PRIMARY KEY,
    airport_code TEXT UNIQUE,
    airport_name TEXT,
    city TEXT,
    timezone TEXT,
    load_timestamp TIMESTAMPTZ DEFAULT now()
);

INSERT INTO mart.dim_airport (airport_code, airport_name, city, timezone)
SELECT airport_code, airport_name, city, timezone
FROM staging.airports
ON CONFLICT (airport_code) DO UPDATE SET
    airport_name = EXCLUDED.airport_name,
    city = EXCLUDED.city,
    timezone = EXCLUDED.timezone,
    load_timestamp = now();

-- Dim Aircrafts
CREATE TABLE IF NOT EXISTS mart.dim_aircraft (
    aircraft_sk SERIAL PRIMARY KEY,
    aircraft_code TEXT UNIQUE,
    model TEXT,
    range INTEGER,
    load_timestamp TIMESTAMPTZ DEFAULT now()
);

INSERT INTO mart.dim_aircraft (aircraft_code, model, range)
SELECT aircraft_code, model, "range"
FROM staging.aircrafts
ON CONFLICT (aircraft_code) DO UPDATE SET
    model = EXCLUDED.model,
    range = EXCLUDED.range,
    load_timestamp = now();

-- Dim Passenger
CREATE TABLE IF NOT EXISTS mart.dim_passenger (
    passenger_sk SERIAL PRIMARY KEY,
    passenger_id TEXT UNIQUE,
    passenger_name TEXT,
    email TEXT,
    phone TEXT,
    load_timestamp TIMESTAMPTZ DEFAULT now()
);

INSERT INTO mart.dim_passenger (passenger_id, passenger_name, email, phone)
SELECT 
    passenger_id, 
    passenger_name, 
    jsonb_extract_path_text(contact_data, 'email') AS email,
    jsonb_extract_path_text(contact_data, 'phone') AS phone
FROM staging.tickets
ON CONFLICT (passenger_id) DO UPDATE SET
    passenger_name = EXCLUDED.passenger_name,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    load_timestamp = now();
