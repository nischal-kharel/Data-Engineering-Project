#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector

ODB_CONFIG = {
    "host": "localhost",
    "port": 13306,
    "user": "deuser",
    "password": "depassword",
    "database": "odb"
}

DW_CONFIG = {
    "host": "localhost",
    "port": 23306,
    "user": "root",
    "password": "secret",
    "database": "dw"
}

def get_or_create(cursor, select_sql, insert_sql, select_vals, insert_vals):
    cursor.execute(select_sql, select_vals)
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(insert_sql, insert_vals)
    return cursor.lastrowid

def main():
    odb_conn = mysql.connector.connect(**ODB_CONFIG)
    odb_cursor = odb_conn.cursor(dictionary=True)

    dw_conn = mysql.connector.connect(**DW_CONFIG)
    dw_cursor = dw_conn.cursor()

    odb_cursor.execute("SELECT * FROM incident")
    rows = odb_cursor.fetchall()

    print(f"Loading {len(rows)} rows into DW...")

    for row in rows:
        dw_cursor.execute("SELECT FactKey FROM FactIncident WHERE eventid = %s", (row["eventid"],))
        if dw_cursor.fetchone():
            continue

        date_key = get_or_create(
            dw_cursor,
            "SELECT DateKey FROM DimDate WHERE iyear=%s AND imonth=%s AND iday=%s",
            "INSERT INTO DimDate (iyear, imonth, iday) VALUES (%s, %s, %s)",
            (row["iyear"], row["imonth"], row["iday"]),
            (row["iyear"], row["imonth"], row["iday"])
        )

        location_key = get_or_create(
            dw_cursor,
            "SELECT LocationKey FROM DimLocation WHERE country=%s AND region=%s AND city=%s AND latitude=%s AND longitude=%s",
            "INSERT INTO DimLocation (country, region, city, latitude, longitude) VALUES (%s, %s, %s, %s, %s)",
            (row["country"], row["region"], row["city"], row["latitude"], row["longitude"]),
            (row["country"], row["region"], row["city"], row["latitude"], row["longitude"])
        )

        attack_key = get_or_create(
            dw_cursor,
            "SELECT AttackKey FROM DimAttack WHERE attacktype=%s",
            "INSERT INTO DimAttack (attacktype) VALUES (%s)",
            (row["attacktype"],),
            (row["attacktype"],)
        )

        target_key = get_or_create(
            dw_cursor,
            "SELECT TargetKey FROM DimTarget WHERE targettype=%s",
            "INSERT INTO DimTarget (targettype) VALUES (%s)",
            (row["targettype"],),
            (row["targettype"],)
        )

        group_key = get_or_create(
            dw_cursor,
            "SELECT GroupKey FROM DimGroupName WHERE groupname=%s",
            "INSERT INTO DimGroupName (groupname) VALUES (%s)",
            (row["groupname"],),
            (row["groupname"],)
        )

        weapon_key = get_or_create(
            dw_cursor,
            "SELECT WeaponKey FROM DimWeapon WHERE weapontype=%s",
            "INSERT INTO DimWeapon (weapontype) VALUES (%s)",
            (row["weapontype"],),
            (row["weapontype"],)
        )

        dw_cursor.execute("""
            INSERT INTO FactIncident (
                eventid, DateKey, LocationKey, AttackKey, TargetKey,
                GroupKey, WeaponKey, nkill, nwound, success, suicide, property, propvalue
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row["eventid"],
            date_key,
            location_key,
            attack_key,
            target_key,
            group_key,
            weapon_key,
            row["nkill"],
            row["nwound"],
            row["success"],
            row["suicide"],
            row["property"],
            row["propvalue"]
        ))

    dw_conn.commit()
    print("DW load complete.")

    odb_cursor.close()
    odb_conn.close()
    dw_cursor.close()
    dw_conn.close()

if __name__ == "__main__":
    main()