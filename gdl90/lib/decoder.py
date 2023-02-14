# Copyright (c) 2016 Eric Dey
# https://github.com/etdey/gdl90

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in 
# the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE 
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN 
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import sys
import datetime
from collections import deque
from . import messages
from lib.fcs import crcCheck
from .messagesuat import messageUatToObject
from typing import Callable


class Decoder(object):
    """GDL-90 data link interface decoder class"""

    def __init__(self):
        self.format = 'normal'
        self.uatOutput = False
        self.inputBuffer = bytearray()
        self.messages = deque()
        self.parserSynchronized = False
        self.stats = {
            'msgCount' : 0,
            'resync' : 0,
            'msgs' : { 0 : [0, 0] },
        }
        self.reportFrequency = 10

        self.return_handler = None
        
        # altitude reporting in plotflight mode
        self.altitude = 0
        self.altitudeAge = 9999
        self.altitudeMaxAge = 5
        
        # setup internal time tracking
        self.gpsTimeReceived = False
        self.dayStart = None
        self.currtime = datetime.datetime.utcnow()
        self.heartbeatInterval = datetime.timedelta(seconds=1)
    
    def addReturnHandler(self, handler_func: Callable):
        """Add a return handler function for when a message is decoded"""
        self.return_handler = handler_func

    def addBytes(self, data):
        """add raw input bytes for decode processing"""
        self.inputBuffer.extend(data)
        self._parseMessages()
    
    
    def _log(self, msg):
        sys.stderr.write('decoder.Decoder:' + msg + '\n')
    
    def _parseMessages(self):
        """parse input buffer for all complete messages"""
        if not self.parserSynchronized:
            if not self._resynchronizeParser():
                # false if we empty the input buffer
                return
        
        while True:
            # Check that buffer has enough bytes to use
            if len(self.inputBuffer) < 2:
                #self._log("buffer reached low watermark")
                return
            
            # We expect 0x7e at the head of the buffer
            if self.inputBuffer[0] != 0x7e:
                # failed assertion; we are not synchronized anymore
                #self._log("synchronization lost")
                if not self._resynchronizeParser():
                    # false if we empty the input buffer
                    return
            
            # Look to see if we have an ending 0x7e marker yet
            try:
                i = self.inputBuffer.index(0x7e, 1)
            except ValueError:
                # no end marker found yet
                #self._log("no end marker found; leaving parser for now")
                return
            
            # Extract byte message without markers and delete bytes from buffer
            msg = self.inputBuffer[1:i]
            del(self.inputBuffer[0:i+1])
            
            # Decode the received message
            self._decodeMessage(msg)
        
        return
    
    
    def _resynchronizeParser(self):
        """throw away bytes in buffer until empty or resynchronized
        Return:  true=resynchronized, false=buffer empty & not synced"""
        
        self.parserSynchronized = False
        self.stats['resync'] += 1
        
        while True:
            if len(self.inputBuffer) < 2:
                #self._log("buffer reached low watermark during sync")
                return False
            
            # found end of a message and beginning of next
            if self.inputBuffer[0] == 0x7e and self.inputBuffer[1] == 0x7e:
                # remove end marker from previous message
                del(self.inputBuffer[0:1])
                self.parserSynchronized = True
                #self._log("parser is synchronized (end:start)")
                return True
            
            if self.inputBuffer[0] == 0x7e:
                self.parserSynchronized = True
                #self._log("parser is synchronized (start)")
                return True
            
            # remove everything up to first 0x7e or end of buffer
            try:
                i = self.inputBuffer.index(0x7e)
                #self._log("removing leading bytes before marker")
            except ValueError:
                # did not find 0x7e, so blank the whole buffer
                i = len(self.inputBuffer)
                #self._log("removing all bytes in buffer since no markers")
            #self._log('inputBuffer[0:{:d}]=' % (len(self.inputBuffer)) +str(self.inputBuffer)[:+32])
            del(self.inputBuffer[0:i])
        
        raise Exception("_resynchronizeParser: unexpected reached end")

    
    def _decodeMessage(self, escapedMessage):
        """decode one GDL90 message without the start/end markers"""
        
        rawMsg = self._unescape(escapedMessage)
        if len(rawMsg) < 5:
            return False
        msg = rawMsg[:-2]
        crc = rawMsg[-2:]
        crcValid = crcCheck(msg, crc)
        
        # Create a new entry for this message type if it doesn't exist
        if not msg[0] in self.stats['msgs'].keys():
            self.stats['msgs'][msg[0]] = [0,0]
        
        if not crcValid:
            self.stats['msgs'][msg[0]][1] += 1
            #print "****BAD CRC****"
            return False
        self.stats['msgs'][msg[0]][0] += 1
        m = messages.messageToObject(msg)
        if not m or m is None:
            return False
        
        self.return_handler(m)

        if m.MsgType == 'Heartbeat':
            self.currtime += self.heartbeatInterval
            if self.format == 'normal':
                print('MSG00: s1={:02X}, s2={:02X}, ts={:02X}'.format(m.StatusByte1, m.StatusByte2, m.TimeStamp))
            elif self.format == 'plotflight':
                self.altitudeAge += 1
        
        elif m.MsgType == 'TrafficReport':
            if m.Latitude == 0.00 and m.Longitude == 0.00 and m.NavIntegrityCat == 0:  # no valid position
                pass
            elif self.format == 'normal':
                print('MSG20: {:0.7f} {:0.7f} {:d} {:d} {:d} {:d} {:s}'.format(m.Latitude, m.Longitude, m.HVelocity, m.VVelocity, m.Altitude, m.TrackHeading, m.CallSign))
        
        return True
    
    
    def _unescape(self, msg):
        """unescape 0x7e and 0x7d characters in coded message"""
        msgNew = bytearray()
        escapeValue = 0x7d
        foundEscapeChar = False
        while True:
            try:
                i = msg.index(escapeValue)
                foundEscapeChar = True
                msgNew.extend(msg[0:i]); # everything up to the escape character
                
                # this will throw an exception if nothing follows the escape
                escapedValue = msg[i+1] ^ 0x20
                msgNew.append(escapedValue); # escaped value
                del(msg[0:i+2]); # remove prefix bytes, escape, and escaped value
                
            except (ValueError, IndexError):
                # no more escape characters
                if foundEscapeChar:
                    msgNew.extend(msg)
                    return msgNew
                else:
                    return msg
        
        raise Exception("_unescape: unexpected reached end")
    
    
    def _messageHex(self, msg, prefix="", suffix="", maxbytes=32, breakint=4):
        """prints the hex contents of a message"""
        s = ""
        numbytes=len(msg)
        if numbytes > maxbytes:  numbytes=maxbytes
        for i in range(numbytes):
            s += "{:02X}" % (msg[i])
            if ((i+1) % breakint) == 0:
                s += " "
        return "{:s}{:s}{:s}" % (prefix, s.strip(), suffix)
    
