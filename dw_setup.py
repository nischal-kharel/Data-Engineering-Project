#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Make sure you have mysql-connector installed:
    pip install mysql-connector-python
Also, make sure you created a mysql user deuser with password depassword and
granted your user all privileges
"""

import mysql.connector
from mysql.connector import Error


def prepare_dw():
    # Connect to MySQL database
    conn = None

    create_db = "CREATE DATABASE IF NOT EXISTS dw"
    use_db = "USE dw"

    drop_tables = [
        "DROP TABLE IF EXISTS FactIncident",
        "DROP TABLE IF EXISTS DimDate",
        "DROP TABLE IF EXISTS DimLocation",
        "DROP TABLE IF EXISTS DimAttack",
        "DROP TABLE IF EXISTS DimTarget",
        "DROP TABLE IF EXISTS DimGroupName",
        "DROP TABLE IF EXISTS DimWeapon",
    ]

    create_dim_date = """
        CREATE TABLE DimDate (
            DateKey     INT AUTO_INCREMENT PRIMARY KEY,
            iyear       INT,
            imonth      INT,
            iday        INT,
            UNIQUE (iyear, imonth, iday)
        )
    """

    create_dim_location = """
        CREATE TABLE DimLocation (
            LocationKey INT AUTO_INCREMENT PRIMARY KEY,
            country     VARCHAR(60),
            region      VARCHAR(60),
            city        VARCHAR(100),
            latitude    DECIMAL(9,6),
            longitude   DECIMAL(9,6),
            UNIQUE (country, region, city, latitude, longitude)
        )
    """

    create_dim_attack = """
        CREATE TABLE DimAttack (
            AttackKey   INT AUTO_INCREMENT PRIMARY KEY,
            attacktype  VARCHAR(60) UNIQUE
        )
    """

    create_dim_target = """
        CREATE TABLE DimTarget (
            TargetKey   INT AUTO_INCREMENT PRIMARY KEY,
            targettype  VARCHAR(60) UNIQUE
        )
    """

    create_dim_group = """
        CREATE TABLE DimGroupName (
            GroupKey    INT AUTO_INCREMENT PRIMARY KEY,
            groupname   VARCHAR(100) UNIQUE
        )
    """

    create_dim_weapon = """
        CREATE TABLE DimWeapon (
            WeaponKey   INT AUTO_INCREMENT PRIMARY KEY,
            weapontype  VARCHAR(60) UNIQUE
        )
    """

    create_fact_incident = """
        CREATE TABLE FactIncident (
            FactKey     INT AUTO_INCREMENT PRIMARY KEY,
            eventid     BIGINT UNIQUE,
            DateKey     INT,
            LocationKey INT,
            AttackKey   INT,
            TargetKey   INT,
            GroupKey    INT,
            WeaponKey   INT,
            nkill       REAL,
            nwound      REAL,
            success     TINYINT,
            suicide     TINYINT,
            property    TINYINT,
            propvalue   REAL,
            FOREIGN KEY (DateKey)     REFERENCES DimDate(DateKey),
            FOREIGN KEY (LocationKey) REFERENCES DimLocation(LocationKey),
            FOREIGN KEY (AttackKey)   REFERENCES DimAttack(AttackKey),
            FOREIGN KEY (TargetKey)   REFERENCES DimTarget(TargetKey),
            FOREIGN KEY (GroupKey)    REFERENCES DimGroupName(GroupKey),
            FOREIGN KEY (WeaponKey)   REFERENCES DimWeapon(WeaponKey)
        )
    """

    try:
        conn = mysql.connector.connect(
            host='localhost',
            port=23306,
            user='deuser',
            password='depassword'
        )

        if conn.is_connected():
            print('Connected to MySQL database')
            cursor = conn.cursor()

            cursor.execute(create_db)
            cursor.execute(use_db)

            # Drop existing tables in reverse dependency order
            for stmt in drop_tables:
                cursor.execute(stmt)

            # Create dimension tables
            cursor.execute(create_dim_date)
            cursor.execute(create_dim_location)
            cursor.execute(create_dim_attack)
            cursor.execute(create_dim_target)
            cursor.execute(create_dim_group)
            cursor.execute(create_dim_weapon)

            # Create fact table
            cursor.execute(create_fact_incident)

            conn.commit()
            print('DW is prepared')

    except Error as e:
        print(e)

    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()


if __name__ == '__main__':
    prepare_dw()
