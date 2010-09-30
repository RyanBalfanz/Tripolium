#!/usr/bin/env python
# encoding: utf-8
"""
logging.py

Created by Balfanz, Ryan on 2010-09-29.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import logging
# import logging.handlers

from settings import DEBUG


if DEBUG:
	LOG_FILENAME = 'debug.log.out.txt'
	LOG_LEVEL = logging.DEBUG
else:
	LOG_FILENAME = 'sqlmr.log.out.txt'
	LOG_LEVEL = logging.INFO

# # Set up a specific logger with our desired output level
# logger = logging.getLogger('Tripolium')
# formatter = logging.Formatter("%(name)s,%(levelname)s,%(message)s")
# # formatter = logging.Formatter("%(asctime)s,%(name)s,%(levelname)s,%(message)s")
# 
# # Add the log message handler to the logger
# rotFileHandler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=1024, backupCount=1)
# conHandler = logging.StreamHandler()
# 
# rotFileHandler.setFormatter(formatter)
# conHandler.setFormatter(formatter)
# 
# logger.setLevel(LOG_LEVEL)
# logger.addHandler(rotFileHandler)
# logger.addHandler(conHandler)
