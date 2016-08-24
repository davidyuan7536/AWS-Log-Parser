import psycopg2
import logging
import csv
import sys
import io
import re

class LogParser():

    def __init__(self):
        pass

    # Exceptions
    class ParseError(Exception):
        pass
    class WrongNumberOfFieldsError(Exception):
        pass
    class RemoteAddressError(Exception):
        pass
    class TimeStampError(Exception):
        pass
    class RequestTimeError(Exception):
        pass
    class RequestMethodError(Exception):
        pass
    class StatusError(Exception):
        pass
    class RequestLengthError(Exception):
        pass
    class BodyBytesSentError(Exception):
        pass
    class BytesSentError(Exception):
        pass
    class MissingArgumentError(Exception):
        pass

    # check sanity before parsing for one row.
    # return a list of 12 strings if valid, or throws an according exception
    def parseRow(self, toParse):

        f = io.StringIO(toParse)
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            if len(row) != 12:
                # logging.error("Number of Fields Doesn't Match")
                raise self.WrongNumberOfFieldsError("Number of Fields Doesn't Match")

            #  0 remote_addr varchar
            if not (re.match('^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$', row[0])):
                raise self.RemoteAddressError("invalid remote_addr: " + row[0])

            #  1 time_iso8601 timestamp
            if not (re.match('^[0-9]{4}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]\+[0-9]{2}:[0-9]{2}$', row[1])):
                raise self.TimeStampError("invalid timestamp: " + row[1])
            #  2 request_time real
            if not (re.match('^[0-9]+\.[0-9]{3}$', row[2])):
                raise self.RequestTimeError("invalid request_time: " + row[2])
            #  3 request_uri varchar
            #  4 request_method varchar
            if (row[4] == ""):
                raise self.RequestMethodError("empty request_method " + row[4])
            #  5 status integer
            if not (re.match('^[1-5][0-9]{2}$', row[5])):
                raise self.StatusError("invalid status: " + row[5])
            #  6 request_length integer
            if not (re.match('^[0-9]+$', row[6])):
                raise self.RequestLengthError("invalid request_length: " + row[6])
            #  7 body_bytes_sent integer
            if not (re.match('^[0-9]+$', row[7])):
                raise self.BodyBytesSentError("invalid body_bytes_sent: " + row[7])
            #  8 bytes_sent integer
            if not (re.match('^[0-9]+$', row[8])):
                raise self.BytesSentError("invalid bytes_sent: " + row[8])
            #  9 http_host varchar
            #  10 http_referer varchar
            #  11 http_user_agent varchar
            return row

    # takes a row and a cursor and tries to insert into table al
    def loadRow(self,tuple, cursor):
        try:
            row = self.parseRow(tuple)

            try:
                cursor.execute("INSERT INTO access_log (remote_addr,time_iso8601,request_time,request_uri,request_method,status,request_length,body_bytes_sent,bytes_sent,http_host,http_referer,http_user_agent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11]))
            except (psycopg2.DatabaseError) as e:
                raise psycopg2.DatabaseError(e)

        except (self.WrongNumberOfFieldsError, self.RemoteAddressError, self.TimeStampError, self.RequestTimeError, self.RequestMethodError, self.StatusError, self.RequestLengthError, self.BodyBytesSentError, self.BytesSentError) as e:
            raise self.ParseError(e)

    # takes a string and tries to parse and insert every row into table db.al
    # complete the successful rows and logs the unseccesful ones for reference
    # returns nothing
    def loadString(self, stringToLoad, host_name="v-dev-ubusvr-1", db_name="cloud_access_logs", user_name="ryan.dai", password="ryan.dai"):
        stringToLoad = stringToLoad.rstrip()
        if stringToLoad == "":
            return
        connection = None
        try:
            connection = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (db_name, user_name, host_name, password))
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute("SET search_path TO dbo,public;")
            log = ""
            linenumber = 0
            f = stringToLoad.splitlines()

            for row in f:
                linenumber += 1
                try:
                    self.loadRow(str(row), cursor)
                except (psycopg2.DatabaseError, self.ParseError) as e:
                    log += "line " + str(linenumber) + ': ' + str(e) + '\n'
            if log != "":
                logging.error(log)

        except psycopg2.DatabaseError as e:
            raise psycopg2.DatabaseError(e)
        finally:
            if connection:
                connection.close()

    # takes a file and tries to parse and insert every row into table db.al
    # complete the successful rows and logs the unseccesful ones for reference
    # returns nothing
    def loadFile(self, host_name="v-dev-ubusvr-1", db_name="cloud_access_logs", user_name="ryan.dai", password="ryan.dai"):
        if len(sys.argv) != 2:
            raise self.MissingArgumentError('One log file needed')
        try:
            file = open(sys.argv[1], 'rt')
            try:
                connection = None
                try:
                    connection = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (db_name, user_name, host_name, password))
                    connection.autocommit = True
                    cursor = connection.cursor()
                    cursor.execute("SET search_path TO dbo,public;")
                    log = ""
                    linenumber = 0

                    for row in file:
                        linenumber += 1
                        try:
                            self.loadRow(str(row), cursor)
                        except (psycopg2.DatabaseError, self.ParseError) as e:
                            log += "line " + str(linenumber) + ': ' + str(e) + '\n'

                    if log != "":
                        logging.error(log)

                except psycopg2.DatabaseError as e:
                    raise psycopg2.DatabaseError(e)
                finally:
                    if connection:
                        connection.close()
            finally:
                file.close()
        except FileNotFoundError:
            raise FileNotFoundError

    # same functionality as loadFile
    # more efficient, less robust
    # EXCEPT using cvs library for parsing - no manual sanity check
    def loadFile2(self, host_name="v-dev-ubusvr-1", db_name="cloud_access_logs", user_name="ryan.dai", password="ryan.dai"):
        if len(sys.argv) != 2:
            raise self.MissingArgumentError('One log file needed')
        try:
            file = open(sys.argv[1], 'rt')
            try:
                reader = csv.reader(file)
                connection = None
                try:
                    connection = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (db_name, user_name, host_name, password))
                    connection.autocommit = True
                    cursor = connection.cursor()
                    cursor.execute("SET search_path TO dbo,public;")
                    log = ""
                    linenumber = 0

                    for row in reader:
                        linenumber += 1
                        try:
                            if len(row) == 12:
                                cursor.execute("INSERT INTO acess_log (remote_addr,time_iso8601,request_time,request_uri,request_method,status,request_length,body_bytes_sent,bytes_sent,http_host,http_referer,http_user_agent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11]))
                            else:
                                log += "line " + str(linenumber) + ": missing column\n"
                        except (psycopg2.DatabaseError) as e:
                            log += "line " + str(linenumber) + ': ' + str(e) + '\n'

                    if log != "":
                        logging.error(log)

                except psycopg2.DatabaseError as e:
                    raise psycopg2.DatabaseError(e)
                finally:
                    if connection:
                        connection.close()
            finally:
                file.close()
        except FileNotFoundError:
            raise FileNotFoundError


if __name__ == '__main__':
    lp = LogParser()
    lp.loadFile()
