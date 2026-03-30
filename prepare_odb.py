#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Global Terrorism Database - Operational Database Setup

Make sure you have mysql-connector installed:
    pip install mysql-connector-python

Make sure you created a MySQL user and granted privileges:
    CREATE USER 'deuser'@'%' IDENTIFIED BY 'depassword';
    GRANT ALL PRIVILEGES ON odb.* TO 'deuser'@'%';
"""

import mysql.connector
from mysql.connector import Error

def prepare_odb():
    conn = None

    create_db = "CREATE DATABASE odb"
    use_db = "USE odb"

    # Each row in the ODB corresponds to one terrorism incident from the GTD dataset.
    # Fields are drawn directly from the GTD CSV columns most relevant to analysis.
    # nkill and nwound use REAL to accommodate GTD's decimal values (some are averaged).
    # propvalue uses REAL because property damage estimates can be very large floats.
    create_table = (
        "CREATE TABLE incident ("
        "  incidentId  INT          NOT NULL AUTO_INCREMENT PRIMARY KEY, "
        "  eventid     BIGINT, "           # GTD unique 12-digit event identifier
        "  iyear       INT, "              # Year of attack
        "  imonth      INT, "              # Month of attack (0 = unknown)
        "  iday        INT, "              # Day of attack (0 = unknown)
        "  country     VARCHAR(60), "      # Country name (country_txt)
        "  region      VARCHAR(60), "      # World region (region_txt)
        "  city        VARCHAR(100), "     # City of attack
        "  latitude    DECIMAL(9,6), "     # GPS latitude
        "  longitude   DECIMAL(9,6), "     # GPS longitude
        "  attacktype  VARCHAR(60), "      # Type of attack (attacktype1_txt)
        "  targettype  VARCHAR(60), "      # Type of target (targtype1_txt)
        "  groupname   VARCHAR(100), "     # Perpetrator group name (gname)
        "  weapontype  VARCHAR(60), "      # Weapon type (weaptype1_txt)
        "  nkill       REAL, "             # Number of people killed
        "  nwound      REAL, "             # Number of people wounded
        "  success     TINYINT, "          # 1 = attack was successful
        "  suicide     TINYINT, "          # 1 = suicide attack
        "  property    TINYINT, "          # 1 = property was damaged
        "  propvalue   REAL"               # Estimated property damage in USD
        ")"
    )

    try:
        conn = mysql.connector.connect(
            host='localhost',
            port=13306,
            user='deuser',
            password='depassword'
        )

        if conn.is_connected():
            print('Connected to MySQL ODB server')

        cursor = conn.cursor()
        cursor.execute(create_db)
        cursor.execute(use_db)
        cursor.execute(create_table)
        conn.commit()

        print('ODB is prepared — database: odb, table: incident')

    except Error as e:
        print(e)

    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == '__main__':
    prepare_odb()
