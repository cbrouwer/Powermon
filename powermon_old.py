#!/usr/bin/env python
"""
    PowerMon
    
    Reads information from the P1 interface, and optionally stores it in a 
    persistence backend. Furthermore it stores some derived metrics, such as 
    consumption per interval.
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

from time import sleep
from datetime import datetime
import pytz
import serial
import sys
import logging
from persistence import mongoPersistence
from helpers import reading

class p1Interface():
    """p1Interface class: handling the serial interface and such"""
    def __init__ (self):
        self.serial_connection = self.getSerialConnection()
        self.tz =  pytz.timezone("Europe/Amsterdam")
        try:
            self.serial_connection.open()
        except Exception:
            logging.exception("Exception on opening serial connection!");
            sys.exit ("No serial connection - I quit!")

    def getSerialConnection(self):
        ser = serial.Serial()
        ser.baudrate = 115200
        ser.bytesize=serial.SEVENBITS
        ser.parity=serial.PARITY_EVEN
        ser.stopbits=serial.STOPBITS_ONE
        ser.xonxoff=0
        ser.rtscts=0
        ser.timeout=20
        ser.port="/dev/ttyUSB0"
        return ser
        
    def getReading(self):
        """ Will block until a full reading is received from the serial interface!
            If the reading is incomplete (e.g. partial data from the serial interface), 
            it will return None """
        self.reading = reading()
        data_left = 1;
        while (data_left > 0):
            try:
                p1_raw = self.serial_connection.readline()
                self.processLine(str(p1_raw))
            except Exception:
                logging.exception("Exception on retrieving data from serial interface!"
                                 + " Trying to continue")
            # Sleep for just a moment, otherwise data_left will always just be 0
            sleep(0.1)
            data_left = self.serial_connection.inWaiting()
        
        # When we get here, there is no more data left. Check if the reading we have is
        # complete. If so, return it!
        if self.reading.isComplete():
            logging.debug("Received a reading at " , self.reading.timestamp)
            logging.debug("usage: ", self.reading.consumption)
            logging.debug("t1: ", self.reading.t1)
            logging.debug("t2: ", self.reading.t2)
            return self.reading;
        else:
            logging.warning("Ignoring invalid reading!")
        return None
        
        
    def processLine(self, line):
        # The serial interface seems to be adding NULL characters sometimes...
        line=line.replace("\x00", "")
        if "0-0:1.0.0" in line:
            i_start = line.index('(')
            i_end = line.index(')')
            if (line.find('W') > -1):
               i_end = line.index('W')
            try:
               date = datetime.strptime( line[i_start+1:i_end], '%y%m%d%H%M%SS')
            except ValueError:
              date = datetime.strptime( line[i_start+1:i_end], '%y%m%d%H%M%S')
            self.reading.timestamp = self.tz.localize(date).astimezone(pytz.utc)
        elif "1-0:1.7.0" in line:
            i_start = line.index('(')
            i_end = line.index('*')
            self.reading.consumption = float(line[i_start+1:i_end])
        elif "1-0:1.8.1" in line:
            i_start = line.index('(')
            i_end = line.index('*')
            self.reading.t1 = float(line[i_start+1:i_end])
        elif "1-0:1.8.2" in line:
            i_start = line.index('(')
            i_end = line.index('*')
            self.reading.t2 = float(line[i_start+1:i_end])
        #elif "kWh" in line:
        #    print "ignoring: ", line


class powermon():
    """ Powermon will request readings from the p1 interface, and process them """
    def __init__ (self):
        self.persistence = mongoPersistence()
        self.p1 = p1Interface()
    
    def start(self):
        while True:
            reading = self.p1.getReading()
            if reading is not None:
               self.persistence.storeReading(reading)
               self.persistence.updateMetrics(reading)
               
instance = powermon()
instance.start()
