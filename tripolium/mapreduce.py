#!/usr/bin/env python
# encoding: utf-8
"""
mapreduce.py

Created by Balfanz, Ryan on 2010-08-20.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.

A Python package for creating SQL/MR functions to be used with Aster Data's nCluster.
"""

import random
import sys
try:
	import cStringIO as StringIO
except ImportError:
	import StringIO

DEBUG = True

COL_DELIMITER = '\t'
ROW_DELIMITER = '\n'
		
		
class BaseMapper(object):
	"""This is the base class for a mapper."""
	def __init__(self, colDelimiter=None, rowDelimiter=None):
		self.colDelimiter = colDelimiter if colDelimiter else COL_DELIMITER
		self.rowDelimiter = rowDelimiter if rowDelimiter else ROW_DELIMITER
			
	def __call__(self,key,value):
		raise NotImplementedError("%s must implement its __call__ method." % (self.__class__.__name__,))
		
		
class IdentityMapper(BaseMapper):
	"""The indentity mapper."""
	def __call__(self,key,value):
		yield key, value
		
		
class UpperMapper(BaseMapper):
	"""Emits rows in all caps."""
	def __call__(self,key,value):
		u, v = key, value.upper()
		yield u, v
		
		
class ReverseMapper(BaseMapper):
	"""Emits rows in all caps."""
	def __call__(self,key,value):
		u, v = key, value[::-1]
		yield u, v
		
		
class SampleMapper(BaseMapper):
	"""Sample input rows."""
	def __init__(self, sampleProb=None, *args, **kwargs):
		super(SampleMapper, self).__init__(*args, **kwargs)
		self.sampleProb = sampleProb if sampleProb else 1.0
		
	def __call__(self,key,value):
		if random.random() <= self.sampleProb:
			yield key, value
			
			
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
		
	mapper = UpperMapper()
	for i, line in enumerate(source):
		for k, v in mapper(i, line):
			sys.stderr.write("%d: %s\n" % (i, (k, v)))
			