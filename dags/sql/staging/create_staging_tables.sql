-- Staging: Flights
DROP TABLE IF EXISTS staging.flights;
CREATE TABLE staging.flights AS
SELECT
    flight_id,
    flight_no,
    scheduled_departure::timestamptz AS scheduled_departure,
    scheduled_arrival::timestamptz AS scheduled_arrival,
    actual_departure::timestamptz AS actual_departure,
    actual_arrival::timestamptz AS actual_arrival,
    UPPER(TRIM(status)) AS status,
    aircraft_code,
    departure_airport,
    arrival_airport,
    -- Derived Metrics
    EXTRACT(EPOCH FROM (actual_departure::timestamptz - scheduled_departure::timestamptz))/60 AS departure_delay_minutes,
    EXTRACT(EPOCH FROM (actual_arrival::timestamptz - scheduled_arrival::timestamptz))/60 AS arrival_delay_minutes,
    now() AS load_timestamp
FROM raw.flights;

-- Staging: Airports
DROP TABLE IF EXISTS staging.airports;
CREATE TABLE staging.airports AS
SELECT
    airport_code,
    TRIM(airport_name) AS airport_name,
    TRIM(city) AS city,
    coordinates,
    timezone,
    now() AS load_timestamp
FROM raw.airports;

-- Staging: Aircrafts
DROP TABLE IF EXISTS staging.aircrafts;
CREATE TABLE staging.aircrafts AS
SELECT
    aircraft_code,
    -- Assuming model might be JSON/JSONB. If text, try to treat as such or just take it.
    -- For safety in demo data which often has {"en": "..."}:
    CASE 
        WHEN LEFT(model::text, 1) = '{' THEN jsonb_extract_path_text(model::jsonb, 'en') 
        ELSE model::text 
    END AS model,
    "range",
    now() AS load_timestamp
FROM raw.aircrafts;

-- Staging: Tickets
DROP TABLE IF EXISTS staging.tickets;
CREATE TABLE staging.tickets AS
SELECT
    ticket_no,
    book_ref,
    passenger_id,
    UPPER(TRIM(passenger_name)) AS passenger_name,
    contact_data::jsonb AS contact_data,
    now() AS load_timestamp
FROM raw.tickets;

-- Staging: Bookings
DROP TABLE IF EXISTS staging.bookings;
CREATE TABLE staging.bookings AS
SELECT
    book_ref,
    book_date::timestamptz AS book_date,
    total_amount::numeric(10, 2) AS total_amount,
    now() AS load_timestamp
FROM raw.bookings;

-- Staging: Ticket Flights
DROP TABLE IF EXISTS staging.ticket_flights;
CREATE TABLE staging.ticket_flights AS
SELECT
    ticket_no,
    flight_id,
    fare_conditions,
    amount::numeric(10, 2) AS amount,
    now() AS load_timestamp
FROM raw.ticket_flights;

-- Staging: Boarding Passes
DROP TABLE IF EXISTS staging.boarding_passes;
CREATE TABLE staging.boarding_passes AS
SELECT
    ticket_no,
    flight_id,
    boarding_no,
    seat_no,
    now() AS load_timestamp
FROM raw.boarding_passes;
