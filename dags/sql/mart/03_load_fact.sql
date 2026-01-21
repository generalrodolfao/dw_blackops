-- Fact Table
CREATE TABLE IF NOT EXISTS mart.fct_aviation_boardings (
    fact_id BIGSERIAL PRIMARY KEY,
    -- Foreign Keys
    date_sk INT,
    passenger_sk INT,
    aircraft_sk INT,
    departure_airport_sk INT,
    arrival_airport_sk INT,
    
    -- Degenerate Dimensions
    ticket_no TEXT,
    flight_id INT,
    flight_no TEXT,
    fare_conditions TEXT,
    boarding_no TEXT,
    seat_no TEXT,
    status TEXT,
    
    -- Metrics
    amount NUMERIC(10, 2),
    scheduled_departure TIMESTAMPTZ,
    actual_departure TIMESTAMPTZ,
    departure_delay_minutes NUMERIC,
    arrival_delay_minutes NUMERIC,
    flight_duration_minutes NUMERIC,
    
    load_timestamp TIMESTAMPTZ DEFAULT now()
);

TRUNCATE TABLE mart.fct_aviation_boardings;

INSERT INTO mart.fct_aviation_boardings (
    date_sk,
    passenger_sk,
    aircraft_sk,
    departure_airport_sk,
    arrival_airport_sk,
    ticket_no,
    flight_id,
    flight_no,
    fare_conditions,
    boarding_no,
    seat_no,
    status,
    amount,
    scheduled_departure,
    actual_departure,
    departure_delay_minutes,
    arrival_delay_minutes,
    flight_duration_minutes
)
SELECT
    TO_CHAR(f.scheduled_departure, 'yyyymmdd')::INT AS date_sk,
    p.passenger_sk,
    ac.aircraft_sk,
    dep.airport_sk AS departure_airport_sk,
    arr.airport_sk AS arrival_airport_sk,
    
    tf.ticket_no,
    f.flight_id,
    f.flight_no,
    tf.fare_conditions,
    bp.boarding_no,
    bp.seat_no,
    f.status,
    
    tf.amount,
    f.scheduled_departure,
    f.actual_departure,
    f.departure_delay_minutes,
    f.arrival_delay_minutes,
    EXTRACT(EPOCH FROM (f.actual_arrival - f.actual_departure))/60 AS flight_duration_minutes

FROM staging.ticket_flights tf
JOIN staging.flights f ON tf.flight_id = f.flight_id
LEFT JOIN staging.tickets t ON tf.ticket_no = t.ticket_no
LEFT JOIN staging.boarding_passes bp ON tf.ticket_no = bp.ticket_no AND tf.flight_id = bp.flight_id

-- Lookup Dimensions
LEFT JOIN mart.dim_passenger p ON t.passenger_id = p.passenger_id
LEFT JOIN mart.dim_aircraft ac ON f.aircraft_code = ac.aircraft_code
LEFT JOIN mart.dim_airport dep ON f.departure_airport = dep.airport_code
LEFT JOIN mart.dim_airport arr ON f.arrival_airport = arr.airport_code;
