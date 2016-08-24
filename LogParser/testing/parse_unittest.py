#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest
import sys
import os,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

import parse

class Parsing_Tests(unittest.TestCase):
    lp = parse.LogParser()

    def test_correct(self):
        self.assertEqual(self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"'), ['10.64.9.7', '2015-06-12T21:00:08+00:00','0.000', '/HealthCheck', 'OPTIONS', '200', '121', '0', '217', '10.64.10.110', '-', 'ELB-HealthChecker/1.0'])

    def test_num_of_fields(self):
        with self.assertRaises(self.lp.WrongNumberOfFieldsError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.WrongNumberOfFieldsError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",,OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')

    def test_remote_addr(self):
        with self.assertRaises(self.lp.RemoteAddressError):
            self.lp.parseRow(',2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.RemoteAddressError):
            self.lp.parseRow('10.64.97,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.RemoteAddressError):
            self.lp.parseRow('10.64..7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.RemoteAddressError):
            self.lp.parseRow('10.64.9111.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.RemoteAddressError):
            self.lp.parseRow('a10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')

        #  1 time_iso8601 timestamp
    def test_timestamp(self):
        with self.assertRaises(self.lp.TimeStampError):
            self.lp.parseRow('10.64.9.7,,0.000,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.TimeStampError):
            self.lp.parseRow('10.64.9.7,201-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.TimeStampError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:0800:00,0.000,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.TimeStampError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:,0.000,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')

        #  2 request_time real
    def test_request_time(self):
        with self.assertRaises(self.lp.RequestTimeError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.RequestTimeError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.00,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.RequestTimeError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,.001,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.RequestTimeError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.0008,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.RequestTimeError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,8.00a,"/HealthCheck",OPTIONS,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')

        #  3 request_uri varchar
        #  4 request_method varchar
    def test_request_method(self):
        with self.assertRaises(self.lp.RequestMethodError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",,200,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')

        #  5 status integer
    def test_status(self):
        with self.assertRaises(self.lp.StatusError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.StatusError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,600,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.StatusError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,aab,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.StatusError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,89,121,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')

        #  6 request_length integer
    def test_request_length(self):
        with self.assertRaises(self.lp.RequestLengthError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.RequestLengthError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,121.1,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.RequestLengthError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,a,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.RequestLengthError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,-1,0,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')

        #  7 body_bytes_sent integer
    def test_body_bytes_sent(self):
        with self.assertRaises(self.lp.BodyBytesSentError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,121,,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.BodyBytesSentError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,121,aab,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.BodyBytesSentError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,121,-1,217,"10.64.10.110","-","ELB-HealthChecker/1.0"')

        #  8 bytes_sent integer
    def test_bytes_sent(self):
        with self.assertRaises(self.lp.BytesSentError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,121,0,,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.BytesSentError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,121,0,-1,"10.64.10.110","-","ELB-HealthChecker/1.0"')
        with self.assertRaises(self.lp.BytesSentError):
            self.lp.parseRow('10.64.9.7,2015-06-12T21:00:08+00:00,0.000,"/HealthCheck",OPTIONS,200,121,0,a,"10.64.10.110","-","ELB-HealthChecker/1.0"')

if __name__ == '__main__':
    unittest.main()
