#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Global Terrorism Database - Data Producer

Reads the GTD Excel file row by row and sends each incident
to the Kafka 'Data' topic for consumption by odb_consumer.py.

Make sure you have the required libraries installed:
    pip install kafka-python pandas openpyxl

Usage:
    python data_producer.py                  (loads all rows)
    python data_producer.py --limit 100      (loads first 100 rows for testing)
"""

import sys
import pandas as pd
from kafka import KafkaProducer
from time import sleep

# GTD Excel filename — place this file in the same folder as this script
GTD_FILE = 'globalterrorismdb.xlsx'

# Only these columns are extracted from the GTD — matches the ODB incident table
GTD_COLUMNS = [
    'eventid',
    'iyear',
    'imonth',
    'iday',
    'country_txt',
    'region_txt',
    'city',
    'latitude',
    'longitude',
    'attacktype1_txt',
    'targtype1_txt',
    'gname',
    'weaptype1_txt',
    'nkill',
    'nwound',
    'success',
    'suicide',
    'property',
    'propvalue'
]

def load_gtd(limit=None):
    print('Reading GTD Excel file: {}...'.format(GTD_FILE))
    df = pd.read_excel(GTD_FILE, usecols=GTD_COLUMNS, engine='openpyxl')

    # Fill missing numeric values with 0 so the ODB never receives NaN
    numeric_cols = ['nkill', 'nwound', 'propvalue', 'latitude', 'longitude',
                    'success', 'suicide', 'property']
    df[numeric_cols] = df[numeric_cols].fillna(0)

    # Fill missing text values with 'Unknown'
    text_cols = ['country_txt', 'region_txt', 'city',
                 'attacktype1_txt', 'targtype1_txt', 'gname', 'weaptype1_txt']
    df[text_cols] = df[text_cols].fillna('Unknown')

    if limit:
        df = df.head(limit)

    print('Loaded {:,} rows from GTD.'.format(len(df)))
    return df

def produce(broker_addr, limit=None):
    df = load_gtd(limit)

    producer = KafkaProducer(
        bootstrap_servers=broker_addr,
        api_version=(2, 0, 2)
    )

    print('\nSending incidents to Kafka topic "Data"...\n')

    for count, (_, row) in enumerate(df.iterrows(), start=1):
        # Encode row as a pipe-delimited string — pipes avoid conflicts with
        # commas that appear inside GTD text fields like city and group names
        message = '|'.join([
            str(row['eventid']),
            str(row['iyear']),
            str(row['imonth']),
            str(row['iday']),
            str(row['country_txt']),
            str(row['region_txt']),
            str(row['city']),
            str(row['latitude']),
            str(row['longitude']),
            str(row['attacktype1_txt']),
            str(row['targtype1_txt']),
            str(row['gname']),
            str(row['weaptype1_txt']),
            str(row['nkill']),
            str(row['nwound']),
            str(int(row['success'])),
            str(int(row['suicide'])),
            str(int(row['property'])),
            str(row['propvalue'])
        ])

        producer.send('Data', message.encode())
        sleep(0.1)
        print('Produced incident {}: eventid={} | {} | {} | {}'.format(
            count, row['eventid'], row['iyear'], row['country_txt'], row['attacktype1_txt']
        ))

    producer.flush()
    print('\nDone — {:,} incidents sent to Kafka topic "Data".'.format(count))

if __name__ == '__main__':
    broker_addr = 'localhost:29092'

    limit = None
    if '--limit' in sys.argv:
        limit = int(sys.argv[sys.argv.index('--limit') + 1])

    produce(broker_addr, limit)
