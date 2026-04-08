#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector
from kafka import KafkaConsumer

KAFKA_TOPIC = "Data"
KAFKA_BOOTSTRAP = "localhost:29092"

MYSQL_CONFIG = {
    "host": "localhost",
    "port": 13306,
    "user": "deuser",
    "password": "depassword",
    "database": "odb"
}

def safe_int(value, default=0):
    try:
        if value == "" or value is None:
            return default
        return int(float(value))
    except (ValueError, TypeError):
        return default

def safe_float(value, default=0.0):
    try:
        if value == "" or value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def safe_str(value, default="Unknown"):
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip()

def main():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="odb-consumer-group"
    )

    insert_sql = """
    INSERT INTO incident (
        eventid, iyear, imonth, iday, country, region, city,
        latitude, longitude, attacktype, targettype, groupname,
        weapontype, nkill, nwound, success, suicide, property, propvalue
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    print("ODB consumer running...")

    for msg in consumer:
        try:
            data = msg.value.decode("utf-8").split("|")

            if len(data) != 19:
                print(f"Skipping malformed row: {data}")
                continue

            values = (
                safe_int(data[0]),      # eventid
                safe_int(data[1]),      # iyear
                safe_int(data[2]),      # imonth
                safe_int(data[3]),      # iday
                safe_str(data[4]),      # country
                safe_str(data[5]),      # region
                safe_str(data[6]),      # city
                safe_float(data[7]),    # latitude
                safe_float(data[8]),    # longitude
                safe_str(data[9]),      # attacktype
                safe_str(data[10]),     # targettype
                safe_str(data[11]),     # groupname
                safe_str(data[12]),     # weapontype
                safe_float(data[13]),   # nkill
                safe_float(data[14]),   # nwound
                safe_int(data[15]),     # success
                safe_int(data[16]),     # suicide
                safe_int(data[17]),     # property
                safe_float(data[18])    # propvalue
            )

            cursor.execute(insert_sql, values)
            conn.commit()
            print(f"Inserted eventid={values[0]} into ODB")

        except Exception as e:
            print(f"Error processing message: {e}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()