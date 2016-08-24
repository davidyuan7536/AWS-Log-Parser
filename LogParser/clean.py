#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import sys

connection = None
try:
    connection = psycopg2.connect("dbname='cloud_access_logs' user='ryan.dai' host='v-dev-ubusvr-1' password='ryan.dai'")
    cursor = connection.cursor()
    cursor.execute("SET search_path TO dbo,public;")
    cursor.execute("DROP TABLE access_log")
    cursor.execute("CREATE TABLE dbo.access_log (id serial PRIMARY KEY, remote_addr varchar,time_iso8601 timestamp,request_time real,request_uri varchar,request_method varchar,status integer,request_length integer,body_bytes_sent integer,bytes_sent integer,http_host varchar,http_referer varchar,http_user_agent varchar);")
    connection.commit()

except psycopg2.DatabaseError as e:
    print ('Error: %s',e)
    sys.exit(1)

finally:
    if connection:
        connection.close()
