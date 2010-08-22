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

# Tripolium modules
from settings import DEBUG, COL_DELIMITER, ROW_DELIMITER
from utils import emitRow

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
	"""This is the base class for a mapper.
	
	:type key: int
	:param key: The 0-based index of the input row
	
	:type value: string
	:param key: The string representation of the input row
	
	:rtype: none
	:return: Using this mapper directly will caise an Exception to be thrown
	"""
	def __init__(self, colDelimiter=None, rowDelimiter=None, schema=None, *args, **kwargs):
		self.colDelimiter = colDelimiter if colDelimiter else COL_DELIMITER
		self.rowDelimiter = rowDelimiter if rowDelimiter else ROW_DELIMITER
		if DEBUG:
			import schema as ss
			self.meta = getattr(ss, 'DEBUG_SCHEMA')
		if schema:
			import schema as s
			self.meta = getattr(s, schema)
		else:
			self.meta = None
		# self.meta = kwargs if kwargs else None
		# logger.debug("Meta: %s" % (str(self.meta),))
		self.columns = {} if self.meta else None
		
	def __call__(self,key,value):
		raise NotImplementedError("%s must implement its __call__ method." % (self.__class__.__name__,))
		
	def process_raw_columns(self, raw=None):
		"""Process raw columns data into a dictionary
		
		:rtype: dict
		:return: A dictionary mapping string column names to their typed values
		"""
		assert self.meta
		fields = raw.split(COL_DELIMITER)
		# logger.debug("Fields: %s" % (str(map(str, fields))),)
		
		for k, v in self.meta.iteritems():
			# logger.debug("k: v; %s: %s" % (k, v,))
			colData = fields[v["index"]-1]
			colType = v["type"]
			# logger.debug("k, colData, colType: %s, %s, %s" % (k, colData, colType))
			try:
				self.columns[k] = map(colType, (colData,))[0]
				# logger.debug("Processed field '%s' into %s: %s" % (k, type(self.columns[k]), self.columns[k]))
			except Exception, e:
				raise Exception(e)
				
				
		
class IdentityMapper(BaseMapper):
	"""The indentity mapper.
	
	:type key: int
	:param key: The 0-based index of the input row
	
	:type value: string
	:param key: The string representation of the input row
	
	:rtype: tuple
	:return: A 2-tuple containing a key, value equal to the input key, value pair
	"""
	def __call__(self, key, value):
		yield key, value
		
		
class ReverseMapper(BaseMapper):
	"""Emits rows in all caps.
	
	:type key: int
	:param key: The 0-based index of the input row
	
	:type value: string
	:param key: The string representation of the input row
	
	:rtype: tuple
	:return: A 2-tuple containing a key, value pair where value is the reversed version of the input value
	"""
	def __call__(self, key, value):
		u, v = key, value[::-1]
		yield u, v
		
		
class SampleMapper(BaseMapper):
	"""Sample input rows.
	
	:type key: int
	:param key: The 0-based index of the input row
	
	:type value: string
	:param key: The string representation of the input row
	
	:rtype: tuple
	:return: A 2-tuple containing a key, value pair where value is the original uppercase version of the input value
	
	:SQL/MR parameters:
		:type sampleProb: float
		:param partitionColumnIndex: The sampling probability
		:default: 1.0
	"""
	def __init__(self, sampleProb=None, *args, **kwargs):
		super(SampleMapper, self).__init__(*args, **kwargs)
		if not sampleProb:
			sampleProb = 1.0
			logger.debug("%s was not given a sampling probability, using %f" % (self.__class__.__name__, sampleProb,))
		self.sampleProb = float(sampleProb)
		logger.debug("%s.sampleProb is %f" % (self.__class__.__name__, self.sampleProb,))
		
		
	def __call__(self, key, value):
		if random.random() <= self.sampleProb:
			yield key, value
			
			
class SchemaMapper(BaseMapper):
	"""The schema indentity mapper.

	:type key: int
	:param key: The 0-based index of the input row

	:type value: string
	:param key: The string representation of the input row

	:rtype: tuple
	:return: A 2-tuple containing a key, value equal to the input key, value pair
	"""
	def __call__(self, key, value):
		self.process_raw_columns(value)
		yield key, str(self.columns)
			
			
class SessionMapper(BaseMapper):
	"""A mapper to sessionize events.
	
	:type key: int
	:param key: The 0-based index of the input row
	
	:rtype: tuple
	:return: A 2-tuple containing a key, value pair.
	
	:SQL/MR parameters:
		:type timeout: int
		:param timeout: The session timeout in seconds
		:default: 60
		
		:type fmt: string
		:param fmt: The format specifier of the datetime column
		:default: '%Y-%m-%d %H:%M:%S'
	"""
	def __init__(self, timeout=60, fmt=None, *args, **kwargs):
		super(SessionMapper, self).__init__(*args, **kwargs)
		self.sessionTimeoutSeconds = timedelta(seconds=int(timeout))
		self.dateTimeFormat = fmt if fmt else "%Y-%m-%d %H:%M:%S"
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
		
		
class ZenoSampleMapper(SampleMapper):
	"""Sample input rows, after each hit the sampling probability is halved.
	
	:type key: int
	:param key: The 0-based index of the input row
	
	:type value: string
	:param key: The string representation of the input row
	
	:rtype: unknown
	:return: A 2-tuple key, value pair when appropriate
	"""
	# def __init__(self, sampleProb=None, *args, **kwargs):
	# 	super(ZenoSampleMapper, self).__init__(*args, **kwargs)
	# 	if not sampleProb:
	# 		sampleProb = 1.0
	# 		logger.debug("%s was not given a sampling probability, using %f" % (self.__class__.__name__, sampleProb,))
	# 	self.sampleProb = sampleProb
	# 	logger.debug("%s.sampleProb is %f" % (self.__class__.__name__, self.sampleProb,))
	# 	
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
	"""Emits rows in all caps.
	
	:type key: int
	:param key: The 0-based index of the input row
	
	:type value: string
	:param key: The string representation of the input row
	
	:rtype: tuple
	:return: A 2-tuple containing a key, value pair where value is the original uppercase version of the input value
	"""
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
		users.append(rowTemplate.format(userId=1001, firstName="Paul", lastName="Dirac", dob="1902-10-20 00:00:00"))
		users.append(rowTemplate.format(userId=1002, firstName="Albert", lastName="Einstein", dob="1955-04-18 00:00:0"))
		users.append(rowTemplate.format(userId=1003, firstName="Richard", lastName="Feynman", dob="1918-05-11 00:00:0"))
		users.append(rowTemplate.format(userId=1004, firstName="Erwin", lastName="Schrodinger", dob="1887-08-12 00:00:0"))
		source = StringIO.StringIO(ROW_DELIMITER.join(users) + ROW_DELIMITER)
	else:
		source = sys.stdin
		
	argsDict = {}
	dynamicMapper = None
	if len(sys.argv) > 1:
		try:
			if hasattr(globals()[sys.argv[1]], '__call__'):
				dynamicMapper = globals()[sys.argv[1]]
				className = sys.argv.pop(1)
		except KeyError, e:
			logging.error("%s is missing" % (e,))
			sys.exit()
		logger.debug("parsing args")
		for arg in sys.argv[1:]:
			option, value = arg.split('=')
			assert option.startswith('--')
			argsDict[option[2:]] = value
		logger.debug("argsDict: %s" % (argsDict,))
		
	mapper = dynamicMapper(**argsDict) if dynamicMapper else IdentityMapper(**argsDict)
	# mapper = ZenoSampleMapper(sampleProb=1.0)
	logger.info("Created new %s instance with extra kwargs %s" % (mapper.__class__.__name__, str(argsDict)))
	for i, line in enumerate(source):
		i, line = i, line.rstrip(ROW_DELIMITER)
		logger.debug("%s input: {k:'%d', v:'%s'}" % (mapper.__class__.__name__, i, line))
		for k, v in mapper(i, line):
			emitRow(COL_DELIMITER.join(map(str, [k, v])))
			