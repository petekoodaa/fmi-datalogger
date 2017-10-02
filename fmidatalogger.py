#!/usr/bin/env python

import datetime
import fmiopendata
import logging
import os
import psycopg2
import sys
import time

TIME_INTERVAL = 15*60

DBNAME = os.environ["DBNAME"]
DBUSER = os.environ["DBUSER"]
DBTABLE = 'temperature'

log = logging.getLogger(__name__)
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s', datefmt="%Y-%m-%d %H:%M:%S"))
out_hdlr.setLevel(logging.INFO)
log.addHandler(out_hdlr)
log.setLevel(logging.INFO)

def main():

    running = True
    location = "Kirkkonummi"

    db_create_table()

    while running:
        # Fetch temperatures
        fmidata = fetch_temperature(location)

        # Log and write to database
        for d in fmidata:
            for t, val in zip(d.t, d.data):
                log.info("Temperature in {} at {}: {} UTC".format(location, t.strftime("%Y-%m-%d %H:%M:%S"), val))
                db_write(location, val, t)

        # Sleep
        time.sleep(TIME_INTERVAL)

def fetch_temperature(location):
    try:
        fmiod = fmiopendata.FMIOpenData()
    except Keyerror:
        log.error("Failed to initialize FMI Open Data interface\nWon't be able to fetch temperatures.")
        return []

    try:
        fmidata = fmiod.get_data(location, "temperature")
    except fmiopendata.FMIError as e:
        log.error("Failed to fetch temperature\n\n{}".format(e.msg))
        fmidata = []
    finally:
        return fmidata

def db_connect():

    RETRIES = 5

    while RETRIES:
        try:
            return psycopg2.connect("dbname='{}' user='{}'".format(DBNAME, DBUSER))
        except:
            RETRIES -= 1
            log.warning("Unable to connect to database, {} retries left".format(RETRIES))
            time.sleep(2)

    log.error("Failed to connect to database")
    return None

def db_create_table():
    conn = db_connect()

    if conn:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS {} (
            location varchar(255),
            temperature real,
            time timestamp
            );""".format(DBTABLE))

        cur.close()
        conn.commit()
        conn.close()

def db_write(location, temperature, t):
    sql = """
        INSERT INTO {} (location, temperature, time)
            VALUES ('{}', {}, '{}');
        """

    conn = db_connect()

    if conn:
        cur = conn.cursor()
        cur.execute(sql.format(DBTABLE, location, temperature, t.strftime("%Y-%m-%dT%H:%M:%SZ")));
        cur.close()
        conn.commit()
        conn.close()
    else:
        log.error("Failed to write data to db (connection failed)")

if __name__=="__main__":
    main()
