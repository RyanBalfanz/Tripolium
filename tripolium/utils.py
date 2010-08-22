import csv
import sys

# Tripolium modules
import settings
# from logging.loggers import logger

def csv_to_db(csvFile=None):
	"""Generate db rows from csv input."""	
	with open(csvFile, 'rb') as f:
		csvReader = csv.reader(f)
		for row in csvReader:
			yield settings.COL_DELIMITER.join(row)
			
def emitRow(s=None):
	"""Emit string row, appending row delimiter."""
	# logger.debug(s + settings.ROW_DELIMITER)
	sys.stdout.write(s + settings.ROW_DELIMITER)
	
if __name__ == "__main__":
	for row in csv_to_db("user.csv"):
		emitRow(row)