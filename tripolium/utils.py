import csv
import sys

# from mapreduce.settings import COL_DELIMITER, ROW_DELIMITER

# Tripolium modules
from settings import COL_DELIMITER, ROW_DELIMITER
# from logging.loggers import logger

def csv_to_db(csvFile=None):
	"""Generate db rows from csv input."""	
	with open(csvFile, 'rb') as f:
		csvReader = csv.reader(f)
		for row in csvReader:
			yield COL_DELIMITER.join(row)
			
def emitRow(s=None):
	"""Emit string row, appending row delimiter."""
	# logger.debug(s + settings.ROW_DELIMITER)
	sys.stdout.write(s + ROW_DELIMITER)
	
if __name__ == "__main__":
	for row in csv_to_db("user.csv"):
		emitRow(row)
		