import psycopg
import time

class Analytics:

    def __init__(self, args: dict):
        # args is a dict of string passed with the --args flag
        # user passed a yaml/json, in python that's a dict object
        self.delay: int = int(args.get("delay", 100))

        # you can arbitrarely add any variables you want
        self.counter: int = 0



    # the setup() function is executed only once
    # when a new executing thread is started.
    # Also, the function is a vector to receive the excuting threads's unique id and the total thread count
    def setup(self, conn: psycopg.Connection, id: int, total_thread_count: int):
        self.id = id
        conn.autocommit = True
        with conn.cursor() as cur:
            print(
                f"My thread ID is {id}. The total count of threads is {total_thread_count}"
            )
            print(cur.execute(f"select version()").fetchone()[0])



    # the run() function returns a list of functions
    # that dbworkload will execute, sequentially.
    # Once every func has been executed, run() is re-evaluated.
    # This process continues until dbworkload exits.
    def loop(self):   
        return [self.queryRecord,
                self.scheduleRollup,
                self.airlineRollup,
                self.departureRollup,
                self.arrivalRollup,
                self.details]



    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def queryRecord(self, conn: psycopg.Connection):
        query = f"""
SELECT airline_id, departure_airport, arrival_airport, model
FROM flight_snapshot
ORDER BY random()
LIMIT 1;
"""
        with conn.cursor() as cur:
            cur.execute("BEGIN TRANSACTION AS OF SYSTEM TIME follower_read_timestamp();")
            cur.execute(query)
            self.airline, self.departure, self.arrival, self.model = cur.fetchone()
            cur.execute("COMMIT;")



    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def scheduleRollup(self, conn: psycopg.Connection):
        query = "select * from getScheduleRollup();"
        with conn.cursor() as cur:
            cur.execute("BEGIN TRANSACTION AS OF SYSTEM TIME follower_read_timestamp();")
            cur.execute(query)
            cur.execute("COMMIT;")
        time.sleep(self.delay / 1000)



    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def airlineRollup(self, conn: psycopg.Connection):
        query = f"select * from getAirlineRollup('{self.airline}');"
        with conn.cursor() as cur:
            cur.execute("BEGIN TRANSACTION AS OF SYSTEM TIME follower_read_timestamp();")
            cur.execute(query)
            cur.execute("COMMIT;")
        time.sleep(self.delay / 1000)



    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def departureRollup(self, conn: psycopg.Connection):
        query = f"""
select * from getDepartureRollup(
  '{self.airline}',
  '{self.departure}'
);
"""
        with conn.cursor() as cur:
            cur.execute("BEGIN TRANSACTION AS OF SYSTEM TIME follower_read_timestamp();")
            cur.execute(query)
            cur.execute("COMMIT;")
        time.sleep(self.delay / 1000)



    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def arrivalRollup(self, conn: psycopg.Connection):
        query = f"""
select * from getArrivalRollup(
  '{self.airline}',
  '{self.departure}',
  '{self.arrival}'
);
"""
        with conn.cursor() as cur:
            cur.execute("BEGIN TRANSACTION AS OF SYSTEM TIME follower_read_timestamp();")
            cur.execute(query)
            cur.execute("COMMIT;")
        time.sleep(self.delay / 1000)



    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def details(self, conn: psycopg.Connection):
        query = f"""
select * from getFlightDetails(
  '{self.airline}',
  '{self.departure}',
  '{self.arrival}',
  '{self.model}'
);
"""
        with conn.cursor() as cur:
            cur.execute("BEGIN TRANSACTION AS OF SYSTEM TIME follower_read_timestamp();")
            cur.execute(query)
            cur.execute("COMMIT;")
        time.sleep(self.delay / 1000)
