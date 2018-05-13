# Author: Omkar Sunkersett
# Purpose: To fetch ISONE data and update the database
# Summer Internship at Argonne National Laboratory

import csv, datetime, MySQLdb, os, requests, time

class ISONE():
	def __init__(self, base_url, file_nom, prog_dir):
		self.base_url = base_url
		self.file_nom = file_nom
		self.prog_dir = prog_dir
		self.files_cached = []

	def fetch_files(self, file_dt):
		try:
			path_struc = '\\' + file_dt[:4] + '\\' + file_dt[4:6] + '\\' + file_dt[6:]
			if os.path.isdir(self.prog_dir + '\\cache\\iso-ne' + path_struc) == False:
				os.makedirs(self.prog_dir + '\\cache\\iso-ne' + path_struc)
			os.chdir(self.prog_dir + '\\cache\\iso-ne' + path_struc)
			response = requests.get(self.base_url, params = {'start': file_dt})
			fwrite = open(self.file_nom + file_dt + '.csv', 'wb')
			fwrite.write(response.content)
			fwrite.close()	
			self.files_cached.append(self.prog_dir + '\\cache\\iso-ne' + path_struc + '\\' + self.file_nom + file_dt + '.csv')
			print (self.prog_dir + '\\cache\\iso-ne' + path_struc + '\\' + self.file_nom + file_dt + '.csv')
			os.chdir(self.prog_dir)
		except Exception as e:
			print (str(e))

	def etl_file_data(self, cache_file):
		try:
			fread = open(cache_file, 'r')
			flines = [x.rstrip('\n') for x in fread.readlines() if x.endswith('.csv\n')]
			fread.close()
			cnx = MySQLdb.connect(user = 'not-published', passwd = 'not-published', host = 'not-published', db = 'not-published')
			cursor = cnx.cursor()
			cursor.execute("SELECT market_id FROM market_meta USE INDEX (PRIMARY) WHERE market_name = 'ISO-NE'")
			mkt_id = cursor.fetchone()[0]
			i = 1
			for fname in flines:
				print ('Current file: ' + fname + '\t' + 'Percent complete: ' + str(round((float(i)*100)/len(flines), 2)) + ' %')
				fread = open(fname, 'r')
				frows = csv.reader(fread, delimiter = ',')
				offer_base_rs = []
				ins_perf = True
				for row in frows:
					if len(row) > 0 and row[0] == 'D' and row[2].isdigit() == True:
						if ins_perf == True:
							cursor.execute("SELECT offer_id, identifier_1, identifier_2 FROM offer_base USE INDEX (IDX_OFFER_BASE_MARKET_ID) WHERE market_id = %s", (mkt_id,))
							offer_base_rs = list(cursor.fetchall())
							if len(offer_base_rs) > 0:
								off_check = [x for (x, y, z) in offer_base_rs if (row[3], row[4]) == (y, z)]
								if len(off_check) > 0:
									off_id = off_check[0]
									ins_perf = False
								else:
									cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[3], row[4], "ISO-NE", mkt_id))
									ins_perf = True
									cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[3], row[4]))
									off_id = cursor.fetchone()[0]
							else:
								cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[3], row[4], "ISO-NE", mkt_id))
								ins_perf = True
								cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[3], row[4]))
								off_id = cursor.fetchone()[0]
						else:
							off_check = [x for (x, y, z) in offer_base_rs if (row[3], row[4]) == (y, z)]
							if len(off_check) > 0:
								off_id = off_check[0]
								ins_perf = False
							else:
								cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[3], row[4], "ISO-NE", mkt_id))
								ins_perf = True
								cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[3], row[4]))
								off_id = cursor.fetchone()[0]

						mrun_id = 'DAM'
						intv_dt = row[1].split('/')[2] + '-' + row[1].split('/')[0].zfill(2) + '-' + row[1].split('/')[1].zfill(2)
						intv_start = intv_dt + ' ' + str(int(row[2])-1).zfill(2) + ':00:00'
						intv_end = (datetime.datetime.strptime(intv_start, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours = 1, minutes = 0)).strftime("%Y-%m-%d %H:%M:%S")
						hr, iv = int(row[2]), 0
						intv_id = str(off_id) + '-' + mrun_id + '-' + intv_start[2:4] + intv_start[5:7] + intv_start[8:10] + intv_start[11:13] + intv_start[14:16]

						cursor.execute("INSERT INTO interval_meta (interval_id, offer_id, market_id, mkt_run_id, interval_dt, interval_start, interval_end, opr_hour, opr_interval) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (intv_id, off_id, mkt_id, mrun_id, intv_dt, intv_start, intv_end, hr, iv))
						cursor.execute("INSERT INTO isone_results (interval_id, reglimit_low, reglimit_high, reg_status, autoresp_rate, regoff_price, regserv_price, regcap_price, regito_cost) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (intv_id, float(row[5].zfill(1)), float(row[6].zfill(1)), row[7], float(row[8].zfill(1)), float(row[9].zfill(1)), float(row[10].zfill(1)), float(row[11].zfill(1)), float(row[12].zfill(1))))
				cnx.commit()
				fread.close()
				i += 1
			cursor.close()
			cnx.close()
		except Exception as e:
			print (str(e))

	def __str__(self):
		try:
			os.chdir(self.prog_dir + '\\cache\\iso-ne')
			fwrite = open('isone-cache.txt', 'w')
			fwrite.write('File(s) cached are as follows:\n')
			for file_name in self.files_cached:
				fwrite.write(file_name + '\n')
			fwrite.close()
			os.chdir(self.prog_dir)
			return ("File(s) cached: " + ', '.join(self.files_cached) + '\n')
		except Exception as e:
			print (str(e))

def dbdt_check(mkt_name, start_dt, end_dt):
	try:
		print ("\nStarting the database date validation check...\n")
		cnx = MySQLdb.connect(user = 'not-published', passwd = 'not-published', host = 'not-published', db = 'not-published')
		cursor = cnx.cursor()
		cursor.execute("SELECT min(interval_dt) AS oldest_dt, max(interval_dt) AS latest_dt FROM interval_meta USE INDEX (IDX_INTERVAL_META_MARKET_ID) WHERE market_id = (SELECT DISTINCT market_id FROM market_meta USE INDEX (PRIMARY) WHERE lower(market_name) = %s)", (mkt_name.lower(),))
		rs = cursor.fetchone()
		cursor.close()
		cnx.close()
		print("Database Oldest Date (MM-DD-YYYY): " + datetime.datetime.strftime(rs[0], "%m-%d-%Y"))
		dbdt_start = datetime.datetime.strptime(datetime.datetime.strftime(rs[0], "%Y-%m-%d"), "%Y-%m-%d")
		print("Database Latest Date (MM-DD-YYYY): " + datetime.datetime.strftime(rs[1], "%m-%d-%Y"))
		dbdt_end = datetime.datetime.strptime(datetime.datetime.strftime(rs[1], "%Y-%m-%d"), "%Y-%m-%d")
		print("Script Start Date (MM-DD-YYYY): " + start_dt)
		start_dt = datetime.datetime.strptime(start_dt.split('-')[2] + '-' + start_dt.split('-')[0] + '-' + start_dt.split('-')[1], "%Y-%m-%d")
		print("Script End Date (MM-DD-YYYY): " + end_dt)
		end_dt = datetime.datetime.strptime(end_dt.split('-')[2] + '-' + end_dt.split('-')[0] + '-' + end_dt.split('-')[1], "%Y-%m-%d")
		if start_dt == (dbdt_end + datetime.timedelta(hours = 24, minutes = 0)) and end_dt >= start_dt and end_dt <= datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(hours = 24, minutes = 0), "%Y-%m-%d"), "%Y-%m-%d"):
			print ("\nThe database date validation check has completed successfully. The program will now execute...\n")
			return True
		else:
			actual_st = datetime.datetime.strftime(dbdt_end + datetime.timedelta(hours = 24, minutes = 0), "%Y-%m-%d")
			actual_ed = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(hours = 24, minutes = 0), "%Y-%m-%d")
			print ("\nPlease check the script start and end dates properly. The start date must be set to " + actual_st.split('-')[1] + '-' + actual_st.split('-')[2] + '-' + actual_st.split('-')[0] + " (MM-DD-YYYY) and the end date must be less than or equal to " + actual_ed.split('-')[1] + '-' + actual_ed.split('-')[2] + '-' + actual_ed.split('-')[0] + " (MM-DD-YYYY) and also not less than the start date.")
			return False
	except Exception as e:
		print (str(e))


def main():
	print ("\n********** Start of the Program **********\n")

	base_url = "https://www.iso-ne.com/transform/csv/hbregulationoffer?"
	
	# prog_dir is the main directory under which the CSV files will be stored
	#prog_dir = "C:\\Users\\Omkar Sunkersett\\Downloads\\markets"

	reg_offers = ISONE(base_url, 'hbregulationoffer_', prog_dir)

	# These respective variables set the start and end dates for fetching data from the server
	#startdatetime = "MM-DD-YYYY"
	#enddatetime = "MM-DD-YYYY"

	if dbdt_check("ISO-NE", startdatetime, enddatetime):
		start_month = int(startdatetime.split('-')[0])
		start_day = int(startdatetime.split('-')[1])
		start_year = int(startdatetime.split('-')[2])
		end_month = int(enddatetime.split('-')[0])
		end_day = int(enddatetime.split('-')[1])
		end_year = int(enddatetime.split('-')[2])

		for year in range(start_year, end_year+1):
			if year == end_year:
				max_months = end_month
			else:
				max_months = 12
			for month in range(start_month, max_months+1):
				if month == end_month and year == end_year:
					max_days = end_day
				elif month in [1, 3, 5, 7, 8, 10, 12]:
					max_days = 31
				elif month in [4, 6, 7, 9, 11]:
					max_days = 30
				elif month == 2 and year % 4 == 0:
					max_days = 29
				else:
					max_days = 28
				for day in range(start_day, max_days+1):
					file_dt = str(year) + str(month).zfill(2) + str(day).zfill(2)
					# Code for fetching the CSV files from the server for all markets
					#reg_offers.fetch_files(file_dt)
				start_day = 1
			start_month = 1

		# The print() function writes the absolute paths of the CSV files to the cache file
		#print (reg_offers)
	
		# Code for loading the CSV data into the not-published database for all markets
		# IMPORTANT: Make sure you have the latest backup of the database before uncommenting the below lines
		#print ("\nLoading the new data into the database...\n")
		#reg_offers.etl_file_data(prog_dir + "\\cache\\iso-ne\\isone-cache.txt")

	print ("\n********** End of the Program **********\n")


main()
