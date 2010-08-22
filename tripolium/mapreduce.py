#!/usr/bin/env python
# encoding: utf-8
"""
mapreduce.py

Created by Balfanz, Ryan on 2010-08-20.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.

A Python package for creating SQL/MR functions to be used with Aster Data's nCluster.
"""

import logging
import logging.handlers
import random
import sys
try:
	import cStringIO as StringIO
except ImportError:
	import StringIO

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
			
			
class UpperMapper(BaseMapper):
	"""Emits rows in all caps."""
	def __call__(self, key, value):
		u, v = key, value.upper()
		yield u, v
		
		
if __name__ == "__main__":
	if DEBUG:
		users = []
		rowTemplate = "{userId}\t{firstName}\t{lastName}\t{dob}"
		users.append(rowTemplate.format(userId=1001, firstName="Paul", lastName="Dirac", dob="1984-10-20"))
		users.append(rowTemplate.format(userId=1002, firstName="Albert", lastName="Einstein", dob="1955-04-18"))
		users.append(rowTemplate.format(userId=1003, firstName="Richard", lastName="Feynman", dob="1918-05-11"))
		users.append(rowTemplate.format(userId=1004, firstName="Erwin", lastName="Schrodinger", dob="1887-08-12"))
		source = StringIO.StringIO(ROW_DELIMITER.join(users))
	else:
		source = sys.stdin
		
	mapper = IdentityMapper()
	for i, line in enumerate(source):
		for k, v in mapper(i, line):
			sys.stderr.write("%d: %s\n" % (i, (k, v)))
			