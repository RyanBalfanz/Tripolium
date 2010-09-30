#!/usr/bin/env python
# encoding: utf-8
"""
mapred.py

Created by Balfanz, Ryan on 2010-09-06.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import sys
import cStringIO as StringIO


class BaseInputFormat(object):
	"""docstring for BaseInputFormat"""
	def __init__(self, inputFile=sys.stdin):
		super(BaseInputFormat, self).__init__()
		self.inputFile = inputFile
		self.inputLineNum = 0
		
	def get_input_file(self):
		"""docstring for get_input_file"""
		return self.inputFile
	FILE = property(get_input_file)
	
	def get_input_line_number(self):
		"""docstring for get_input_line_number"""
		return self.inputLineNum
	LINENUM = property(get_input_line_number)
	
	def generate_input(self):
		"""docstring for generate_input"""
		raise NotImplementedError
				
				
class NClusterPartitionInputFormat(BaseInputFormat):
	"""docstring for NClusterRowInputFormat"""
	def __init__(self, partitionColumnIndex=None, *args, **kwargs):
		super(NClusterPartitionInputFormat, self).__init__(*args, **kwargs)
		self.partitionColumnIndex = partitionColumnIndex
		self.partitionKeys = []
		self.partitions = []
		self.generate_input = self.generate_input_partition
		
	def generate_input_partition(self):
		"""docstring for generate_input_row"""
		from contextlib import closing
		with closing(self.inputFile) as f:
			prevKey = None
			for i, line in enumerate(f):
				self.inputLineNum = i
				line = line.strip()
				pKey = line.split(',')[self.partitionColumnIndex] if self.partitionColumnIndex is not None else line
				if not self.partitionKeys:
					self.partitionKeys.append(pKey)
					self.partitions.append([])
				elif not pKey == prevKey:
					p = self.partitions.pop()
					self.partitions.append([])
					yield (prevKey, p)
				else:
					pass
				self.partitions[-1].append(line)
				prevKey = pKey
			self.inputLineNum += 1
			yield (pKey, self.partitions.pop())
			
			
class NClusterRowInputFormat(NClusterPartitionInputFormat):
	"""docstring for NClusterRowInputFormat"""
	def __init__(self, *args, **kwargs):
		super(NClusterRowInputFormat, self).__init__(*args, **kwargs)
		self.generate_input_row = self.generate_input_partition
				
				
if __name__ == '__main__':
	inputString = """
	1,1,1,1,1,1
	1,1,2,2,2,2
	1,1,2,3,3,3
	2,1,2,3,4,4
	3,1,2,3,4,5
	3,2,1,2,3,4
	"""
	
	inpFile = StringIO.StringIO(inputString.strip())
	inp = BaseInputFormat()
	inp = NClusterRowInputFormat(inputFile=inpFile)
	inp = NClusterPartitionInputFormat(inputFile=inpFile, partitionColumnIndex=5)
	for line in inp.generate_input():
		print "PRINT LINE: ", line, inp.LINENUM
