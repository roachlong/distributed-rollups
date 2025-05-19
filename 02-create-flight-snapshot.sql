USE schedules;

ANALYZE airlines;
ANALYZE airports;
ANALYZE planes;
ANALYZE flights;
ANALYZE flight_status;
ANALYZE seat_inventory;
ANALYZE flight_prices;


CREATE MATERIALIZED VIEW flight_snapshot AS
SELECT
  f.flight_id,
  f.airline_id,
  f.plane_id,
  f.departure_airport,
  f.arrival_airport,
  f.scheduled_departure,
  f.scheduled_arrival,

  -- static lookups
  al.name AS airline_name,
  da.airport_code AS departure_airport_code,
  da.name AS departure_name,
  da.city AS departure_city,
  da.country AS departure_country,
  aa.airport_code AS arrival_airport_code,
  aa.name AS arrival_name,
  aa.city AS arrival_city,
  aa.country AS arrival_country,
  p.model,
  p.capacity,

  -- dynamic snapshot at refresh time
  fs.status,
  si.seats_available,
  fp.price_usd

FROM flights AS f
JOIN airlines AS al ON f.airline_id = al.airline_id
JOIN airports AS da ON f.departure_airport = da.airport_id
JOIN airports AS aa ON f.arrival_airport = aa.airport_id
JOIN planes AS p ON f.plane_id = p.plane_id
LEFT JOIN flight_status AS fs ON f.flight_id = fs.flight_id
LEFT JOIN seat_inventory AS si ON f.flight_id = si.flight_id
LEFT JOIN flight_prices AS fp ON f.flight_id = fp.flight_id

AS OF SYSTEM TIME follower_read_timestamp();



CREATE INDEX idx_snapshot_by_rollup_path
  ON flight_snapshot (airline_id, departure_airport, arrival_airport, model, plane_id)
  STORING (
    airline_name,
    departure_airport_code,
    arrival_airport_code,
    capacity,
    status,
    seats_available,
    price_usd
  );


CREATE INDEX idx_flight_by_updated_at
  ON flights (updated_at)
  STORING (
    airline_id,
    plane_id,
    departure_airport,
    arrival_airport,
    scheduled_departure,
    scheduled_arrival
  );


CREATE INDEX idx_flight_status_updated_at
  ON flight_status (updated_at)
  STORING (
    status
  );


CREATE INDEX idx_seat_inventory_updated_at
  ON seat_inventory (updated_at)
  STORING (
    seats_available
  );


CREATE INDEX idx_flight_prices_updated_at
  ON flight_prices (updated_at)
  STORING (
    price_usd
  );
