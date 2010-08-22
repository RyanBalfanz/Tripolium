#!/usr/bin/env python
# encoding: utf-8
"""
mapreduce.py

Created by Balfanz, Ryan on 2010-08-20.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.

A Python module for creating SQL/MR functions to be used with Aster Data's nCluster.
"""

import logging
import logging.handlers
import random
import sys
try:
	import cStringIO as StringIO
except ImportError:
	import StringIO
from datetime import datetime
from datetime import timedelta

DEBUG = True

COL_DELIMITER = '\t'
ROW_DELIMITER = '\n'
if DEBUG:
	LOG_FILENAME = 'debug.log.out.txt'
	LOG_LEVEL = logging.DEBUG
else:
	LOG_FILENAME = 'sqlmr.log.out.txt'
	LOG_LEVEL = logging.INFO

# Set up a specific logger with our desired output level
logger = logging.getLogger('Tripolium')
formatter = logging.Formatter("%(name)s,%(levelname)s,%(message)s")
# formatter = logging.Formatter("%(asctime)s,%(name)s,%(levelname)s,%(message)s")

# Add the log message handler to the logger
rotFileHandler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=1024, backupCount=1)
conHandler = logging.StreamHandler()

rotFileHandler.setFormatter(formatter)
conHandler.setFormatter(formatter)

logger.setLevel(LOG_LEVEL)
logger.addHandler(rotFileHandler)
logger.addHandler(conHandler)

def emitRow(s):
	"""Emit string row, appending row delimiter."""
	logging.debug(s + ROW_DELIMITER)
	sys.stdout.write(s + ROW_DELIMITER)
		
		
class BaseMapper(object):
	"""This is the base class for a mapper."""
	def __init__(self, colDelimiter=None, rowDelimiter=None):
		self.colDelimiter = colDelimiter if colDelimiter else COL_DELIMITER
		self.rowDelimiter = rowDelimiter if rowDelimiter else ROW_DELIMITER
			
	def __call__(self,key,value):
		raise NotImplementedError("%s must implement its __call__ method." % (self.__class__.__name__,))
		
		
class IdentityMapper(BaseMapper):
	"""The indentity mapper."""
	def __call__(self, key, value):
		yield key, value
		
		
class ReverseMapper(BaseMapper):
	"""Emits rows in all caps."""
	def __call__(self, key, value):
		u, v = key, value[::-1]
		yield u, v
		
		
class SampleMapper(BaseMapper):
	"""Sample input rows."""
	def __init__(self, sampleProb=None, *args, **kwargs):
		super(SampleMapper, self).__init__(*args, **kwargs)
		if not sampleProb:
			sampleProb = 1.0
			logger.debug("%s was not given a sampling probability, using %f" % (self.__class__.__name__, sampleProb,))
		self.sampleProb = sampleProb
		
	def __call__(self, key, value):
		if random.random() <= self.sampleProb:
			yield key, value
			
			
class SessionMapper(BaseMapper):
	"""A mapper to sessionize events."""
	def __init__(self, timeout=60, *args, **kwargs):
		super(SessionMapper, self).__init__(*args, **kwargs)
		self.dateTimeFormat = "%Y-%m-%d %H:%M:%S"
		self.sessionTimeoutSeconds = timedelta(seconds=timeout)
		self.currentSession = {}

	def __call__(self, key, value):
		partitionRowNum, row = key, value

		userId, timeString = row.split(COL_DELIMITER)
		timeString = datetime.strptime(timeString, self.dateTimeFormat)

		if not self.currentSession:
			self.currentSession["number"] = 0
			self.currentSession["start"] = timeString
			self.currentSession["length"] = timedelta(0.0)

		dt = timeString - self.currentSession["start"]
		if dt > self.sessionTimeoutSeconds:
			self.currentSession["number"] += 1
			self.currentSession["start"] = timeString
			self.currentSession["length"] = timedelta(0.0)
		else:
			self.currentSession["length"] = dt

		yield str(userId), str(timeString) + "___" + str(self.currentSession["number"]) + "___" + str(self.currentSession["length"])
		
		
class ZenoSampleMapper(BaseMapper):
	"""Sample input rows."""
	def __init__(self, sampleProb=None, *args, **kwargs):
		super(ZenoSampleMapper, self).__init__(*args, **kwargs)
		if not sampleProb:
			sampleProb = 1.0
			logger.debug("%s was not given a sampling probability, using %f" % (self.__class__.__name__, sampleProb,))
		self.sampleProb = sampleProb

	def __call__(self, key, value):
		if random.random() <= self.sampleProb:
			self.sampleProb = 0.5 * self.sampleProb
			yield key, value + '___' + str(2.0 * self.sampleProb)
			
			
class PartitionMapper(BaseMapper):
	"""A mapper suitable for partitioned input.
	
	Note that nCluster initializes a new instance for each partition.
	The mechanism is more than likely just separate invocations of the script.
	"""
	def __init__(self, timeout=60, *args, **kwargs):
		super(PartitionMapper, self).__init__(*args, **kwargs)
				
	def __call__(self, key, value):
		partitionRowNum, row = key, value
		yield partitionRowNum, row
			
			
class UpperMapper(BaseMapper):
	"""Emits rows in all caps."""
	def __call__(self, key, value):
		u, v = key, value.upper()
		yield u, v
		
		
if __name__ == "__main__":
	if DEBUG:
		users = []
		usersCols = [
			"{userId}",
			"{firstName}",
			"{lastName}",
			"{dob}"
		]
		rowTemplate = COL_DELIMITER.join(usersCols)
		users.append(rowTemplate.format(userId=1001, firstName="Paul", lastName="Dirac", dob="1984-10-20 00:00:00"))
		users.append(rowTemplate.format(userId=1002, firstName="Albert", lastName="Einstein", dob="1955-04-18 00:00:0"))
		users.append(rowTemplate.format(userId=1003, firstName="Richard", lastName="Feynman", dob="1918-05-11 00:00:0"))
		users.append(rowTemplate.format(userId=1004, firstName="Erwin", lastName="Schrodinger", dob="1887-08-12 00:00:0"))
		source = StringIO.StringIO(ROW_DELIMITER.join(users) + ROW_DELIMITER)
	else:
		source = sys.stdin
		
	mapper = IdentityMapper()
	# mapper = ZenoSampleMapper(sampleProb=1.0)
	logger.info("Created new %s instance" % (mapper.__class__.__name__))
	for i, line in enumerate(source):
		i, line = i, line.rstrip(ROW_DELIMITER)
		logger.debug("Mapper input: {k:'%d', v:'%s'}" % (i, line))
		for k, v in mapper(i, line):
			emitRow(COL_DELIMITER.join(map(str, [k, v])))
			