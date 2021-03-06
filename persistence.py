#!/usr/bin/env python
""" 
    Provide persistence for PowerMon
"""
#    Copyright (C) 2016  Chris Brouwer
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

import logging
import sys
from datetime import datetime
from datetime import timedelta
import pytz
import pymongo
from pymongo import MongoClient
from helpers import reading


class mongoPersistence():
    def __init__ (self):
        self.client = None;

        try:
            self.client = MongoClient('mongodb://192.168.1.1:27017/')
        except Exception:
            logging.exception("Error on opening mongo client")
            sys.exit("Exception on getting Persistence - no point in continuing...")
        self.last5mreading = self.getLastMetrics("metrics.minute")
        self.last1Hreading = self.getLastMetrics("metrics.hour")
        self.last1Dreading = self.getLastMetrics("metrics.day")
        self.last1Mreading = self.getLastMetrics("metrics.month")

    def getLastMetrics(self, collection):
        try:
            db = self.client.powermon
            metrics = db[collection]
            cursor = metrics.find().sort('ts', pymongo.DESCENDING).limit(1)
            if cursor.count() > 0:
                next = cursor.next()
                areading = reading()
                areading.t1 = next["t1"]
                areading.t2 = next["t2"]
                areading.timestamp = pytz.UTC.localize(next["ts"])
                return areading
        except Exception:
            logging.exception("Error on retrieving last metrics  in %s!", collection)   
        return None
        
    def storeReading(self, reading):
        try:
            db = self.client.powermon
            mreading = {"ts": reading.timestamp.utcnow(), \
                        "consumption": reading.consumption}
            readings = db.reading
            readings.insert_one(mreading)
        except Exception:
            logging.exception("Error on inserting reading!")
            
    def updateMetrics(self, reading):
        self.updateMetrics5m(reading)
        self.updateMetrics1H(reading) 
        self.updateMetrics1D(reading)
        self.updateMetrics1M(reading)
    
    def updateMetrics5m(self, reading):
        doUpdate = False
        
        logging.debug(self.last5mreading)
        if self.last5mreading is None:
            doUpdate = True
        else:
            # Going to check how old our last metrics is. If it's older than 6 minutes, 
            # then we missed some data. To prevent weird delta's, we clear the last reading
            if (reading.timestamp - self.last5mreading.timestamp) > timedelta(seconds=359):
                doUpdate = True;
                self.last5mreading = None
            elif (reading.timestamp - self.last5mreading.timestamp) > timedelta(seconds=300):
                doUpdate = True;
        if doUpdate:
            logging.info("updating metrics 5m")
            reading.timestamp = reading.timestamp.replace(second=0,microsecond=0)
            self.insertMetrics(reading, self.last5mreading, "metrics.minute")
            self.last5mreading = reading

    def updateMetrics1H(self, reading):
        doUpdate = False
        if self.last1Hreading is None:
            doUpdate = True
        else:
            # Going to check how old our last metrics is. If it's older than 1:05 hours, 
            # then we missed some data. To prevent weird delta's, we clear the last reading
            if (reading.timestamp - self.last1Hreading.timestamp) > timedelta(seconds=3900):
                doUpdate = True;
                self.last1Hreading = None
            elif reading.timestamp.hour != self.last1Hreading.timestamp.hour:
                doUpdate = True;
        if doUpdate:
            logging.info("updating metrics 1H")
            reading.timestamp = reading.timestamp.replace(minute=0,second=0,microsecond=0)
            self.insertMetrics(reading, self.last1Hreading, "metrics.hour")
            self.last1Hreading = reading
       
    def updateMetrics1D(self, reading):
        doUpdate = False
        if self.last1Dreading is None:
            doUpdate = True
        else:
            # Going to check how old our last metrics is. If it's older than 25 hours, 
            # then we missed some data. To prevent weird delta's, we clear the last reading
            if (reading.timestamp - self.last1Dreading.timestamp) >= timedelta(hours=25):
                doUpdate = True;
                self.last1Dreading = None
            elif reading.timestamp.day != self.last1Dreading.timestamp.day:
                doUpdate = True;
        if doUpdate:
            logging.info("updating metrics 1D")
            reading.timestamp = reading.timestamp.replace(hour=0,minute=0,second=0,microsecond=0)
            self.insertMetrics(reading, self.last1Dreading, "metrics.day")
            self.last1Dreading = reading

    def updateMetrics1M(self, reading):
        doUpdate = False
        if self.last1Mreading is None:
            doUpdate = True
        else:
            # Going to check how old our last metrics is. If it's older than 32 days, 
            # then we missed some data. To prevent weird delta's, we clear the last reading
            if (reading.timestamp - self.last1Mreading.timestamp) >= timedelta(days=32):
                doUpdate = True;
                self.last1Mreading = None
            # Compare on month, rather than diff. This takes care of months with different
            # number of days
            elif reading.timestamp.month != self.last1Mreading.timestamp.month:
                doUpdate = True;
        if doUpdate:
            logging.info("updating metrics 1M")
            reading.timestamp = reading.timestamp.replace(hour=0,minute=0,second=0,microsecond=0)
            self.insertMetrics(reading, self.last1Mreading, "metrics.month")
            self.last1Mreading = reading

    def insertMetrics(self, reading, lastreading, collection):
            try:
                db = self.client.powermon
                
                if lastreading is None:
                    delta_t1 = 0
                    delta_t2 = 0
                    delta_total = 0
                else:
                    delta_t1 = round(reading.t1 - lastreading.t1, 5)
                    delta_t2 = round(reading.t2 - lastreading.t2,5)
                    delta_total = round(delta_t1 + delta_t2,5)
                 
                mreading = {"ts":  reading.timestamp, \
                        "t1": reading.t1, \
                        "t2": reading.t2, \
                        "d_t1": delta_t1, \
                        "d_t2": delta_t2, \
                        "d_total": delta_total \
                        }
                metrics = db[collection]
                metrics.insert_one(mreading)
            except Exception:
                logging.exception("Error on inserting metrics  in %s!", collection)   
        
        
       
