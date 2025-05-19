import os
import psycopg
from analytics import Analytics  # make sure this matches the file/module name

conn_string = os.getenv("DATABASE_URL")

args = {
    "delay": 1000
}

analytics = Analytics(args)

with psycopg.connect(conn_string) as conn:
    analytics.setup(conn, id=0, total_thread_count=1)

    for step in analytics.loop():
        step(conn)
