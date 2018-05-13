# Author: Omkar Sunkersett
# Purpose: To fetch MISO data and update the database
# Summer Internship at Argonne National Laboratory

import csv, datetime, MySQLdb, os, requests, time

class MISO():
	def __init__(self, base_url, file_nom, file_ext, prog_dir):
		self.base_url = base_url
		self.file_nom = file_nom
		self.file_ext = file_ext
		self.prog_dir = prog_dir
		self.files_cached = []

	def fetch_files(self, market_run_id, start_dt, end_dt):
		try:
			start_year = int(start_dt.split('-')[2])
			start_month = int(start_dt.split('-')[0])
			start_day = int(start_dt.split('-')[1])
			end_year = int(end_dt.split('-')[2])
			end_month = int(end_dt.split('-')[0])
			end_day = int(end_dt.split('-')[1])

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
						file_name = str(year) + str(month).zfill(2) + str(day).zfill(2) + self.file_nom + self.file_ext
						response = requests.get(self.base_url + '/' + file_name)
						if response.status_code == 200:
							if os.path.isdir(self.prog_dir + '\\cache\\miso\\' + market_run_id + '\\' + str(year) + '\\' + str(month) + '\\' + str(day)) == False:
								os.makedirs(self.prog_dir + '\\cache\\miso\\' + market_run_id + '\\' + str(year) + '\\' + str(month) + '\\' + str(day))
							os.chdir(self.prog_dir + '\\cache\\miso\\' + market_run_id + '\\' + str(year) + '\\' + str(month) + '\\' + str(day))
							fwrite = open(file_name, 'wb')
							fwrite.write(response.content)
							fwrite.close()
							self.files_cached.append(self.prog_dir + '\\cache\\miso\\' + market_run_id + '\\' + str(year) + '\\' + str(month) + '\\' + str(day) + '\\' + file_name)
							print ("Current File: " + self.files_cached[-1])
							time.sleep(6)
							os.chdir(self.prog_dir)
						else:
							print ("File Not Found: " + file_name)
					start_day = 1
				start_month = 1
		except Exception as e:
			print (str(e))

	def etl_file_data(self, cache_file):
		try:
			fread = open(cache_file, 'r')
			flines = [x.rstrip('\n') for x in fread.readlines() if x.endswith('.csv\n')]
			fread.close()
			cnx = MySQLdb.connect(user = 'not-published', passwd = 'not-published', host = 'not-published', db = 'not-published')
			cursor = cnx.cursor()
			cursor.execute("SELECT market_id FROM market_meta USE INDEX (PRIMARY) WHERE market_name = 'MISO'")
			mkt_id = cursor.fetchone()[0]
			i = 1
			for fname in flines:
				print ('Current file: ' + fname + '\t' + 'Percent complete: ' + str(round((float(i)*100)/len(flines), 2)) + ' %')
				fread = open(fname, 'r')
				frows = csv.reader(fread, delimiter = ',')
				next(frows, None)
				offer_base_rs = []
				ins_perf = True
				for row in frows:
					if len(row) > 0:
						if ins_perf == True:
							cursor.execute("SELECT offer_id, identifier_1, identifier_2 FROM offer_base USE INDEX (IDX_OFFER_BASE_MARKET_ID) WHERE market_id = %s", (mkt_id,))
							offer_base_rs = list(cursor.fetchall())
							if len(offer_base_rs) > 0:
								off_check = [x for (x, y, z) in offer_base_rs if (row[1], row[2]) == (y, z)]
								if len(off_check) > 0:
									off_id = off_check[0]
									ins_perf = False
								else:
									cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[1], row[2], row[0], mkt_id))
									ins_perf = True
									cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[1], row[2]))
									off_id = cursor.fetchone()[0]
							else:
								cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[1], row[2], row[0], mkt_id))
								ins_perf = True
								cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[1], row[2]))
								off_id = cursor.fetchone()[0]
						else:
							off_check = [x for (x, y, z) in offer_base_rs if (row[1], row[2]) == (y, z)]
							if len(off_check) > 0:
								off_id = off_check[0]
								ins_perf = False
							else:
								cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[1], row[2], row[0], mkt_id))
								ins_perf = True
								cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[1], row[2]))
								off_id = cursor.fetchone()[0]

						if fname.split('_')[2].lower() =='da':
							mrun_id = 'DAM'
						elif fname.split('_')[2].lower() =='rt':
							mrun_id = 'RTM'
						intv_dt = row[3].split()[0].split('/')[2] + '-' + row[3].split()[0].split('/')[0].zfill(2) + '-' + row[3].split()[0].split('/')[1].zfill(2)
						intv_start = intv_dt + ' ' + row[3].split()[1].split(':')[0].zfill(2) + ':' + row[3].split()[1].split(':')[1].zfill(2) + ':00'
						intv_end = (datetime.datetime.strptime(intv_start, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours = 1, minutes = 0)).strftime("%Y-%m-%d %H:%M:%S")
						hr, iv = (int(row[3].split()[1].split(':')[0]) + 1), 0
						intv_id = str(off_id) + '-' + mrun_id + '-' + intv_start[2:4] + intv_start[5:7] + intv_start[8:10] + intv_start[11:13] + intv_start[14:16]

						cursor.execute("INSERT INTO interval_meta (interval_id, offer_id, market_id, mkt_run_id, interval_dt, interval_start, interval_end, opr_hour, opr_interval) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (intv_id, off_id, mkt_id, mrun_id, intv_dt, intv_start, intv_end, hr, iv))

						if mrun_id == 'DAM':
							cursor.execute("INSERT INTO miso_results (interval_id, reg_max, reg_min, regoff_price, regself_limit, spinoff_price, spinself_limit, onsupp_price, onsuppself_limit, offsupp_price, offsuppself_limit, regavg_mcp, regavg_cap, spinavg_mcp, spinavg_cap, suppavg_mcp, suppavg_cap) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (intv_id, float(row[5].zfill(1)), float(row[6].zfill(1)), float(row[7].zfill(1)), float(row[8].zfill(1)), float(row[9].zfill(1)), float(row[10].zfill(1)), float(row[11].zfill(1)), float(row[12].zfill(1)), float(row[13].zfill(1)), float(row[14].zfill(1)), float(row[15].zfill(1)), float(row[16].zfill(1)), float(row[17].zfill(1)), float(row[18].zfill(1)), float(row[19].zfill(1)), float(row[20].zfill(1))))
						elif mrun_id == 'RTM':
							regmcp = (float(row[14].zfill(1)) + float(row[16].zfill(1)) + float(row[18].zfill(1)) + float(row[20].zfill(1)) + float(row[22].zfill(1)) + float(row[24].zfill(1)) + float(row[26].zfill(1)) + float(row[28].zfill(1)) + float(row[30].zfill(1)) + float(row[32].zfill(1)) + float(row[34].zfill(1)) + float(row[36].zfill(1))) / 12
							regcap = (float(row[15].zfill(1)) + float(row[17].zfill(1)) + float(row[19].zfill(1)) + float(row[21].zfill(1)) + float(row[23].zfill(1)) + float(row[25].zfill(1)) + float(row[27].zfill(1)) + float(row[29].zfill(1)) + float(row[31].zfill(1)) + float(row[33].zfill(1)) + float(row[35].zfill(1)) + float(row[37].zfill(1))) / 12
							spinmcp = (float(row[38].zfill(1)) + float(row[40].zfill(1)) + float(row[42].zfill(1)) + float(row[44].zfill(1)) + float(row[46].zfill(1)) + float(row[48].zfill(1)) + float(row[50].zfill(1)) + float(row[52].zfill(1)) + float(row[54].zfill(1)) + float(row[56].zfill(1)) + float(row[58].zfill(1)) + float(row[60].zfill(1))) / 12
							spincap = (float(row[39].zfill(1)) + float(row[41].zfill(1)) + float(row[43].zfill(1)) + float(row[45].zfill(1)) + float(row[47].zfill(1)) + float(row[49].zfill(1)) + float(row[51].zfill(1)) + float(row[53].zfill(1)) + float(row[55].zfill(1)) + float(row[57].zfill(1)) + float(row[59].zfill(1)) + float(row[61].zfill(1))) / 12
							suppmcp = (float(row[62].zfill(1)) + float(row[64].zfill(1)) + float(row[66].zfill(1)) + float(row[68].zfill(1)) + float(row[70].zfill(1)) + float(row[72].zfill(1)) + float(row[74].zfill(1)) + float(row[76].zfill(1)) + float(row[78].zfill(1)) + float(row[80].zfill(1)) + float(row[82].zfill(1)) + float(row[84].zfill(1))) / 12
							suppcap = (float(row[63].zfill(1)) + float(row[65].zfill(1)) + float(row[67].zfill(1)) + float(row[69].zfill(1)) + float(row[71].zfill(1)) + float(row[73].zfill(1)) + float(row[75].zfill(1)) + float(row[77].zfill(1)) + float(row[79].zfill(1)) + float(row[81].zfill(1)) + float(row[83].zfill(1)) + float(row[85].zfill(1))) / 12
							cursor.execute("INSERT INTO miso_results (interval_id, reg_max, reg_min, regoff_price, regself_limit, spinoff_price, spinself_limit, onsupp_price, onsuppself_limit, offsupp_price, offsuppself_limit, regavg_mcp, regavg_cap, spinavg_mcp, spinavg_cap, suppavg_mcp, suppavg_cap) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (intv_id, float(row[4].zfill(1)), float(row[5].zfill(1)), float(row[6].zfill(1)), float(row[7].zfill(1)), float(row[8].zfill(1)), float(row[9].zfill(1)), float(row[10].zfill(1)), float(row[11].zfill(1)), float(row[12].zfill(1)), float(row[13].zfill(1)), regmcp, regcap, spinmcp, spincap, suppmcp, suppcap))
				cnx.commit()
				fread.close()
				i += 1
			cursor.close()
			cnx.close()
		except Exception as e:
			print (str(e))

	def __str__(self):
		try:
			os.chdir(self.prog_dir + '\\cache\\miso')
			fwrite = open(self.file_nom[1:].replace('_', '-') + '.txt', 'w')
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

	# prog_dir is the main directory under which the CSV files will be stored
	#prog_dir = "C:\\Users\\Omkar Sunkersett\\Downloads\\markets"

	# These respective variables set the start and end dates for fetching data from the server
	#startdatetime = "MM-DD-YYYY"
	#enddatetime = "MM-DD-YYYY"

	if dbdt_check("MISO", startdatetime, enddatetime):
		# Code for fetching the CSV files from the server for both DA and RT markets
		asm_da_off = MISO("https://www.misoenergy.org/Library/Repository/Market Reports", "_asm_da_co", ".csv", prog_dir)
		#asm_da_off.fetch_files("da", startdatetime, enddatetime)
		#print (asm_da_off)

		asm_rt_off = MISO("https://www.misoenergy.org/Library/Repository/Market Reports", "_asm_rt_co", ".csv", prog_dir)
		#asm_rt_off.fetch_files("rt", startdatetime, enddatetime)
		#print (asm_rt_off)

		# Code for loading the CSV data into the not-published database for both DA and RT markets
		# IMPORTANT: Make sure you have the latest backup of the database before uncommenting the below lines
		#print ("\nLoading the new data into the database...\n")
		#asm_da_off.etl_file_data(prog_dir + "\\cache\\miso\\asm-da-co.txt")
		#asm_rt_off.etl_file_data(prog_dir + "\\cache\\miso\\asm-rt-co.txt")

	print ("\n********** End of the Program **********\n")


main()
