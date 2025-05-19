USE schedules;


CREATE OR REPLACE FUNCTION schedules.getScheduleRollup()
RETURNS TABLE (
  key          UUID,
  rollup       TEXT,
  flights      INT,
  on_time      INT,
  delayed      INT,
  cancelled    INT,
  tix_sold     INT,
  open_seats   INT,
  revenue      DECIMAL
) AS $$
DECLARE
  last_refresh TIMESTAMPTZ;
BEGIN
  SELECT created
  INTO last_refresh
  FROM system.jobs
  WHERE description SIMILAR TO '%(CREATE|REFRESH) MATERIALIZED VIEW %flight_snapshot%'
    AND status = 'succeeded'
  ORDER BY finished DESC
  LIMIT 1;
  
  RETURN QUERY 
    WITH live_flights AS (
      SELECT
        flight_id,
        airline_id,
        plane_id,
        departure_airport,
        arrival_airport,
        scheduled_departure,
        scheduled_arrival
      FROM flights
      WHERE updated_at > last_refresh
    ),
    live_status AS (
      SELECT
        flight_id,
        status
      FROM flight_status
      WHERE updated_at > last_refresh
    ),
    live_seats AS (
      SELECT
        flight_id,
        seats_available
      FROM seat_inventory
      WHERE updated_at > last_refresh
    ),
    live_prices AS (
      SELECT
        flight_id,
        price_usd
      FROM flight_prices
      WHERE updated_at > last_refresh
    ),
    overlay AS (
      SELECT
        s.flight_id,
        s.airline_id,
        s.plane_id,
        s.departure_airport,
        s.arrival_airport,
        COALESCE(f.scheduled_departure, s.scheduled_departure) AS scheduled_departure,
        COALESCE(f.scheduled_arrival, s.scheduled_arrival) AS scheduled_arrival,
        s.airline_name,
        s.departure_airport_code,
        s.departure_name,
        s.departure_city,
        s.departure_country,
        s.arrival_airport_code,
        s.arrival_name,
        s.arrival_city,
        s.arrival_country,
        s.model,
        s.capacity,
        COALESCE(t.status, s.status) AS status,
        COALESCE(i.seats_available, s.seats_available) AS seats_available,
        COALESCE(p.price_usd, s.price_usd) AS price_usd

      FROM flight_snapshot AS s
      LEFT JOIN live_flights AS f ON f.flight_id = s.flight_id
      LEFT JOIN live_status AS t ON t.flight_id = s.flight_id
      LEFT JOIN live_seats AS i ON i.flight_id = s.flight_id
      LEFT JOIN live_prices AS p ON p.flight_id = s.flight_id
    )
    SELECT
      airline_id AS key,
      airline_name AS rollup,
      COUNT(1) AS flights,
      COUNT(1) FILTER (WHERE status = 'on_time') AS on_time,
      COUNT(1) FILTER (WHERE status = 'delayed') AS delayed,
      COUNT(1) FILTER (WHERE status = 'cancelled') AS cancelled,
      SUM(capacity - seats_available)::INT AS tix_sold,
      SUM(seats_available)::INT AS open_seats,
      SUM((capacity - seats_available)::DECIMAL * price_usd) AS revenue
    FROM overlay
    GROUP BY airline_id, airline_name
    ORDER BY airline_name;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION schedules.getAirlineRollup(
    p_airline   UUID
)
RETURNS TABLE (
  key          UUID,
  rollup       TEXT,
  flights      INT,
  on_time      INT,
  delayed      INT,
  cancelled    INT,
  tix_sold     INT,
  open_seats   INT,
  revenue      DECIMAL
) AS $$
DECLARE
  last_refresh TIMESTAMPTZ;
BEGIN
  SELECT created
  INTO last_refresh
  FROM system.jobs
  WHERE description SIMILAR TO '%(CREATE|REFRESH) MATERIALIZED VIEW %flight_snapshot%'
    AND status = 'succeeded'
  ORDER BY finished DESC
  LIMIT 1;
  
  RETURN QUERY 
    WITH live_flights AS (
      SELECT
        flight_id,
        airline_id,
        plane_id,
        departure_airport,
        arrival_airport,
        scheduled_departure,
        scheduled_arrival
      FROM flights
      WHERE updated_at > last_refresh
    ),
    live_status AS (
      SELECT
        flight_id,
        status
      FROM flight_status
      WHERE updated_at > last_refresh
    ),
    live_seats AS (
      SELECT
        flight_id,
        seats_available
      FROM seat_inventory
      WHERE updated_at > last_refresh
    ),
    live_prices AS (
      SELECT
        flight_id,
        price_usd
      FROM flight_prices
      WHERE updated_at > last_refresh
    ),
    overlay AS (
      SELECT
        s.flight_id,
        s.airline_id,
        s.plane_id,
        s.departure_airport,
        s.arrival_airport,
        COALESCE(f.scheduled_departure, s.scheduled_departure) AS scheduled_departure,
        COALESCE(f.scheduled_arrival, s.scheduled_arrival) AS scheduled_arrival,
        s.airline_name,
        s.departure_airport_code,
        s.departure_name,
        s.departure_city,
        s.departure_country,
        s.arrival_airport_code,
        s.arrival_name,
        s.arrival_city,
        s.arrival_country,
        s.model,
        s.capacity,
        COALESCE(t.status, s.status) AS status,
        COALESCE(i.seats_available, s.seats_available) AS seats_available,
        COALESCE(p.price_usd, s.price_usd) AS price_usd

      FROM flight_snapshot AS s
      LEFT JOIN live_flights AS f ON f.flight_id = s.flight_id
      LEFT JOIN live_status AS t ON t.flight_id = s.flight_id
      LEFT JOIN live_seats AS i ON i.flight_id = s.flight_id
      LEFT JOIN live_prices AS p ON p.flight_id = s.flight_id
    )
    SELECT
      departure_airport AS key,
      departure_airport_code AS rollup,
      COUNT(1) AS flights,
      COUNT(1) FILTER (WHERE status = 'on_time') AS on_time,
      COUNT(1) FILTER (WHERE status = 'delayed') AS delayed,
      COUNT(1) FILTER (WHERE status = 'cancelled') AS cancelled,
      SUM(capacity - seats_available)::INT AS tix_sold,
      SUM(seats_available)::INT AS open_seats,
      SUM((capacity - seats_available)::DECIMAL * price_usd) AS revenue
    FROM overlay
    WHERE airline_id = p_airline
    GROUP BY departure_airport, departure_airport_code
    ORDER BY departure_airport_code;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION schedules.getDepartureRollup(
  p_airline   UUID,
  p_departure UUID
)
RETURNS TABLE (
  key          UUID,
  rollup       TEXT,
  flights      INT,
  on_time      INT,
  delayed      INT,
  cancelled    INT,
  tix_sold     INT,
  open_seats   INT,
  revenue      DECIMAL
) AS $$
DECLARE
  last_refresh TIMESTAMPTZ;
BEGIN
  SELECT created
  INTO last_refresh
  FROM system.jobs
  WHERE description SIMILAR TO '%(CREATE|REFRESH) MATERIALIZED VIEW %flight_snapshot%'
    AND status = 'succeeded'
  ORDER BY finished DESC
  LIMIT 1;
  
  RETURN QUERY 
    WITH live_flights AS (
      SELECT
        flight_id,
        airline_id,
        plane_id,
        departure_airport,
        arrival_airport,
        scheduled_departure,
        scheduled_arrival
      FROM flights
      WHERE updated_at > last_refresh
    ),
    live_status AS (
      SELECT
        flight_id,
        status
      FROM flight_status
      WHERE updated_at > last_refresh
    ),
    live_seats AS (
      SELECT
        flight_id,
        seats_available
      FROM seat_inventory
      WHERE updated_at > last_refresh
    ),
    live_prices AS (
      SELECT
        flight_id,
        price_usd
      FROM flight_prices
      WHERE updated_at > last_refresh
    ),
    overlay AS (
      SELECT
        s.flight_id,
        s.airline_id,
        s.plane_id,
        s.departure_airport,
        s.arrival_airport,
        COALESCE(f.scheduled_departure, s.scheduled_departure) AS scheduled_departure,
        COALESCE(f.scheduled_arrival, s.scheduled_arrival) AS scheduled_arrival,
        s.airline_name,
        s.departure_airport_code,
        s.departure_name,
        s.departure_city,
        s.departure_country,
        s.arrival_airport_code,
        s.arrival_name,
        s.arrival_city,
        s.arrival_country,
        s.model,
        s.capacity,
        COALESCE(t.status, s.status) AS status,
        COALESCE(i.seats_available, s.seats_available) AS seats_available,
        COALESCE(p.price_usd, s.price_usd) AS price_usd

      FROM flight_snapshot AS s
      LEFT JOIN live_flights AS f ON f.flight_id = s.flight_id
      LEFT JOIN live_status AS t ON t.flight_id = s.flight_id
      LEFT JOIN live_seats AS i ON i.flight_id = s.flight_id
      LEFT JOIN live_prices AS p ON p.flight_id = s.flight_id
    )
    SELECT
      arrival_airport AS key,
      arrival_airport_code AS rollup,
      COUNT(1) AS flights,
      COUNT(1) FILTER (WHERE status = 'on_time') AS on_time,
      COUNT(1) FILTER (WHERE status = 'delayed') AS delayed,
      COUNT(1) FILTER (WHERE status = 'cancelled') AS cancelled,
      SUM(capacity - seats_available)::INT AS tix_sold,
      SUM(seats_available)::INT AS open_seats,
      SUM((capacity - seats_available)::DECIMAL * price_usd) AS revenue
    FROM overlay
    WHERE airline_id = p_airline
      AND departure_airport = p_departure
    GROUP BY arrival_airport, arrival_airport_code
    ORDER BY arrival_airport_code;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION schedules.getArrivalRollup(
  p_airline   UUID,
  p_departure UUID,
  p_arrival   UUID
)
RETURNS TABLE (
  rollup       TEXT,
  flights      INT,
  on_time      INT,
  delayed      INT,
  cancelled    INT,
  tix_sold     INT,
  open_seats   INT,
  revenue      DECIMAL
) AS $$
DECLARE
  last_refresh TIMESTAMPTZ;
BEGIN
  SELECT created
  INTO last_refresh
  FROM system.jobs
  WHERE description SIMILAR TO '%(CREATE|REFRESH) MATERIALIZED VIEW %flight_snapshot%'
    AND status = 'succeeded'
  ORDER BY finished DESC
  LIMIT 1;
  
  RETURN QUERY 
    WITH live_flights AS (
      SELECT
        flight_id,
        airline_id,
        plane_id,
        departure_airport,
        arrival_airport,
        scheduled_departure,
        scheduled_arrival
      FROM flights
      WHERE updated_at > last_refresh
    ),
    live_status AS (
      SELECT
        flight_id,
        status
      FROM flight_status
      WHERE updated_at > last_refresh
    ),
    live_seats AS (
      SELECT
        flight_id,
        seats_available
      FROM seat_inventory
      WHERE updated_at > last_refresh
    ),
    live_prices AS (
      SELECT
        flight_id,
        price_usd
      FROM flight_prices
      WHERE updated_at > last_refresh
    ),
    overlay AS (
      SELECT
        s.flight_id,
        s.airline_id,
        s.plane_id,
        s.departure_airport,
        s.arrival_airport,
        COALESCE(f.scheduled_departure, s.scheduled_departure) AS scheduled_departure,
        COALESCE(f.scheduled_arrival, s.scheduled_arrival) AS scheduled_arrival,
        s.airline_name,
        s.departure_airport_code,
        s.departure_name,
        s.departure_city,
        s.departure_country,
        s.arrival_airport_code,
        s.arrival_name,
        s.arrival_city,
        s.arrival_country,
        s.model,
        s.capacity,
        COALESCE(t.status, s.status) AS status,
        COALESCE(i.seats_available, s.seats_available) AS seats_available,
        COALESCE(p.price_usd, s.price_usd) AS price_usd

      FROM flight_snapshot AS s
      LEFT JOIN live_flights AS f ON f.flight_id = s.flight_id
      LEFT JOIN live_status AS t ON t.flight_id = s.flight_id
      LEFT JOIN live_seats AS i ON i.flight_id = s.flight_id
      LEFT JOIN live_prices AS p ON p.flight_id = s.flight_id
    )
    SELECT
      model AS rollup,
      COUNT(1) AS flights,
      COUNT(1) FILTER (WHERE status = 'on_time') AS on_time,
      COUNT(1) FILTER (WHERE status = 'delayed') AS delayed,
      COUNT(1) FILTER (WHERE status = 'cancelled') AS cancelled,
      SUM(capacity - seats_available)::INT AS tix_sold,
      SUM(seats_available)::INT AS open_seats,
      SUM((capacity - seats_available)::DECIMAL * price_usd) AS revenue
    FROM overlay
    WHERE airline_id = p_airline
      AND departure_airport = p_departure
      AND arrival_airport = p_arrival
    GROUP BY model
    ORDER BY model;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION schedules.getFlightDetails(
  p_airline   UUID,
  p_departure UUID,
  p_arrival   UUID,
  p_model     TEXT
)
RETURNS TABLE (
  scheduled_departure      TIMESTAMPTZ,
  scheduled_arrival        TIMESTAMPTZ,
  airline_name             TEXT,
  departure_airport_code   TEXT,
  arrival_airport_code     TEXT,
  model                    TEXT,
  capacity                 INT,
  status                   TEXT,
  seats_available          INT,
  price_usd                DECIMAL
) AS $$
DECLARE
  last_refresh TIMESTAMPTZ;
BEGIN
  SELECT created
  INTO last_refresh
  FROM system.jobs
  WHERE description SIMILAR TO '%(CREATE|REFRESH) MATERIALIZED VIEW %flight_snapshot%'
    AND status = 'succeeded'
  ORDER BY finished DESC
  LIMIT 1;
  
  RETURN QUERY 
    WITH live_flights AS (
      SELECT
        flight_id,
        airline_id,
        plane_id,
        departure_airport,
        arrival_airport,
        scheduled_departure,
        scheduled_arrival
      FROM flights
      WHERE updated_at > last_refresh
    ),
    live_status AS (
      SELECT
        flight_id,
        status
      FROM flight_status
      WHERE updated_at > last_refresh
    ),
    live_seats AS (
      SELECT
        flight_id,
        seats_available
      FROM seat_inventory
      WHERE updated_at > last_refresh
    ),
    live_prices AS (
      SELECT
        flight_id,
        price_usd
      FROM flight_prices
      WHERE updated_at > last_refresh
    ),
    overlay AS (
      SELECT
        s.flight_id,
        s.airline_id,
        s.plane_id,
        s.departure_airport,
        s.arrival_airport,
        COALESCE(f.scheduled_departure, s.scheduled_departure) AS scheduled_departure,
        COALESCE(f.scheduled_arrival, s.scheduled_arrival) AS scheduled_arrival,
        s.airline_name,
        s.departure_airport_code,
        s.departure_name,
        s.departure_city,
        s.departure_country,
        s.arrival_airport_code,
        s.arrival_name,
        s.arrival_city,
        s.arrival_country,
        s.model,
        s.capacity,
        COALESCE(t.status, s.status) AS status,
        COALESCE(i.seats_available, s.seats_available) AS seats_available,
        COALESCE(p.price_usd, s.price_usd) AS price_usd

      FROM flight_snapshot AS s
      LEFT JOIN live_flights AS f ON f.flight_id = s.flight_id
      LEFT JOIN live_status AS t ON t.flight_id = s.flight_id
      LEFT JOIN live_seats AS i ON i.flight_id = s.flight_id
      LEFT JOIN live_prices AS p ON p.flight_id = s.flight_id
    )
    SELECT
      scheduled_departure,
      scheduled_arrival,
      airline_name,
      departure_airport_code,
      arrival_airport_code,
      model,
      capacity,
      status,
      seats_available,
      price_usd
    FROM overlay
    WHERE airline_id = p_airline
      AND departure_airport = p_departure
      AND arrival_airport = p_arrival
      AND model = p_model
    ORDER BY scheduled_departure;
END;
$$ LANGUAGE plpgsql;
