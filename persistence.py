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
        self.tz =  pytz.timezone("Europe/Amsterdam")

        try:
            self.client = MongoClient('mongodb://192.168.1.1:27017/')
        except Exception:
            logging.exception("Error on opening mongo client")
            sys.exit("Exception on getting Persistence - no point in continuing...")
        self.last1mreading = self.getLastMetrics("metrics.minute");


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
                        "t1": reading.t1, \
                        "t2": reading.t2, \
                        "consumption": reading.consumption}
            readings = db.reading
            readings.insert_one(mreading)
        except Exception:
            logging.exception("Error on inserting reading!")
            
    def updateMetrics(self, reading):
        self.updateMetrics1m(reading);
    
    
    def updateMetrics1m(self, reading):
        if self.last1mreading is None:
            delta = None
        else:
            delta = reading.timestamp - self.last1mreading.timestamp
            if delta > timedelta(minutes=2):
                # We missed some data? To prevent weird deltas, we 
                # reset the delta to zero. A new empty delta will be inserted
                delta = None
        if delta is None or (delta > timedelta(minutes=1)):
            print "updating metrics 1m"
            self.insertMetrics(reading, self.last1mreading, "metrics.minute")
            self.last1mreading = reading
       
    def insertMetrics(self, reading, lastreading, collection):
            try:
                db = self.client.powermon
                
                if lastreading is None:
                    delta_t1 = 0
                    delta_t2 = 0
                    delta_total = 0
                else:
                    delta_t1 = reading.t1 - lastreading.t1
                    delta_t2 = reading.t2 - lastreading.t2
                    delta_total = delta_t1 + delta_t2
                 
                mreading = {"ts":  datetime.utcnow(), \
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
        
        
       