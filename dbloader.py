#!/usr/bin/env python

import adsbexchange

import psycopg2
import psycopg2.extras
import argparse
import pandas as pd
import configparser
import logging
import multiprocessing
import functools


log = logging.getLogger(__name__)

def command_line_args(description):
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('file', type=str, help="file to load")
    parser.add_argument('--config', type=str, dest='config_file',
                        default="loader.conf",
                        help="Loader config file")
    parser.add_argument('--pool', type=int, dest='pool', default=5,
                        help="Pool size for multiprocessing")
    args = parser.parse_args()
    return args


def load_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


type_map = {16: bool,
            23: int,
            20: int,
            701: float,
            1043: str}


def cast_value(column_detail, value):
    if value is '':
        return None

    try:
        return type_map[column_detail.type_code](value)
    except:
        return None


def build_row_data(column_names, record):
    # Initialize an empty dictionary to hold data for this row
    row_data = {c[0]:None for c in column_names.values()}
    columns_not_in_db = []

    # iterate over each record from the data handling it appropriately
    # and adding it to the row_data dictionary
    for column, value in record.items():
        try:
            column_detail = column_names[column.lower()]
            db_column = column_detail[0]
            row_data[db_column] = cast_value(column_detail, value)
            # need to add code to convert types
        except KeyError:
            columns_not_in_db.append(column)

    num_missing = len(columns_not_in_db)

    if num_missing > 0:
        missing_cols = ",".join(columns_not_in_db)
        log.debug("{} columns not in database: {}".format(num_missing, missing_cols))

    return row_data


def get_db_connection(config):
    conn = psycopg2.connect(host=config.get('database', 'host'),
                        port=config.get('database', 'port'),
                        user=config.get('database', 'user'),
                        password=config.get('database', 'password'),
                        dbname=config.get('database', 'database'))
    return conn


def get_column_info(config):
    conn = get_db_connection(config)

    try:
        cursor = conn.cursor()
        cursor.execute("select * from aclist limit 0")
        column_names = [desc for desc in cursor.description]
        return column_names
    finally:
        cursor.close()
        conn.close()


def insert_data_via_copy(cursor, column_names, data):
    log.info("Inserting, using copy, {} records".format(len(data)))
    
    import io
    import csv

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer, delimiter='\t', quoting=csv.QUOTE_MINIMAL)

    for row in data:
        writer.writerow(row)

    string_buffer = io.StringIO(csv_buffer.getvalue())
    cursor.copy_from(string_buffer, 'aclist', columns=['"{}"'.format(x) for x in column_names])


def insert_data(cursor, column_names, data):
    sql_col_string = "({})".format(",".join(['"{}"'.format(c) for c in column_names]))
    sql_query = "insert into aclist {} values %s".format(sql_col_string)
    try:
        log.info("Inserting {} records".format(len(data)))
        psycopg2.extras.execute_values(cursor, sql_query, data)
    except:
        log.exception("Failed to insert")


def load_historical_file(zip_file, column_name_map, config, data_file):
    log.info("Parsing {}/{}".format(zip_file, data_file))
    column_names = [c[0] for c in column_name_map.values()]

    conn = get_db_connection(config)
    cursor = conn.cursor()

    try:
        data = adsbexchange.parse_data(zip_file, data_file)
        values = [build_row_data(column_name_map, x) for x in data['acList']]
        data = [tuple([row[c] for c in column_names]) for row in values]
        insert_data_via_copy(cursor, column_names, data)
    except:
        log.exception("Unable to parse {}/{}".format(zip_file, data_file))
    finally:
        cursor.close()

        conn.commit()
        conn.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    args = command_line_args("Database loader")
    config = load_config(args.config_file)

    log.info("Getting column names from database")
    column_names = {x[0].lower(): x for x in get_column_info(config)}
    #column_names = get_column_names(config)

    log.info("Loading data files")
    file_list = adsbexchange.get_file_list(args.file)

    load_func = functools.partial(load_historical_file, args.file, column_names, config)

    if args.pool > 1:
        pool = multiprocessing.Pool(args.pool)
        results = pool.map(load_func, file_list)
    else:
        results = [load_func(file) for file in file_list]

