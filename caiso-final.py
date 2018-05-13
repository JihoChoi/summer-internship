# Author: Omkar Sunkersett
# Purpose: To fetch CAISO data and update the database
# Summer Internship at Argonne National Laboratory

import csv, datetime, io, MySQLdb, os, requests, time, zipfile

class Caiso():
	def __init__(self, grp_id, start_dt, prog_dir):
		self.fname = []
		self.params = {'groupid' : grp_id, 'startdatetime' : start_dt, 'resultformat' : '6', 'version' : '1'}
		self.prog_dir = prog_dir
		self.dt = start_dt
		self.files_cached = []

	def fetch_files(self, base_url, path_struc):
		try:
			if os.path.isdir(self.prog_dir + '\\cache\\caiso' + path_struc) == False:
				os.makedirs(self.prog_dir + '\\cache\\caiso' + path_struc)
			os.chdir(self.prog_dir + '\\cache\\caiso' + path_struc)
			response = requests.get(base_url, params = self.params)
			print (response.url)
			if 'content-disposition' in response.headers.keys():
				if zipfile.ZipFile(io.BytesIO(response.content)).namelist()[0].endswith('v1.csv'):
					zipfile.ZipFile(io.BytesIO(response.content)).extractall()
					self.fname = [(self.prog_dir + '\\cache\\caiso' + path_struc + '\\' + x) for x in zipfile.ZipFile(io.BytesIO(response.content)).namelist()]
			os.chdir(self.prog_dir)
		except Exception as e:
			print (str(e))

	def etl_file_data(self, cache_file):
		try:
			fread = open(cache_file, 'r')
			flines = [x.rstrip('\n') for x in fread.readlines() if x.endswith('.csv\n') and (("_AS_REQ_" in x[-35:]) or ("_AS_RESULTS_" in x[-35:]) or ("_PRC_AS_" in x[-35:]) or ("_PRC_INTVL_AS_" in x[-35:]))]
			fread.close()
			cnx = MySQLdb.connect(user = 'not-published', passwd = 'not-published', host = 'not-published', db = 'not-published')
			cursor = cnx.cursor()
			cursor.execute("SELECT market_id FROM market_meta USE INDEX (PRIMARY) WHERE market_name = 'CAISO'")
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
								if "_AS_REQ_" in fname[-35:]:
									off_check = [x for (x, y, z) in offer_base_rs if (row[3], row[11]) == (y, z)]
								elif "_AS_RESULTS_" in fname[-35:]:
									off_check = [x for (x, y, z) in offer_base_rs if (row[6], row[4]) == (y, z)]
								elif "_PRC_AS_" in fname[-35:]:
									off_check = [x for (x, y, z) in offer_base_rs if (row[7], row[4]) == (y, z)]
								elif "_PRC_INTVL_AS_" in fname[-35:]:
									off_check = [x for (x, y, z) in offer_base_rs if (row[9], row[10]) == (y, z)]
								if len(off_check) > 0:
									off_id = off_check[0]
									ins_perf = False
								else:
									if "_AS_REQ_" in fname[-35:]:
										cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[3], row[11], row[3], mkt_id))
										cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[3], row[11]))
									elif "_AS_RESULTS_" in fname[-35:]:
										cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[6], row[4], row[6], mkt_id))
										cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[6], row[4]))
									elif "_PRC_AS_" in fname[-35:]:
										cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[7], row[4], row[7], mkt_id))
										cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[7], row[4]))
									elif "_PRC_INTVL_AS_" in fname[-35:]:
										cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[9], row[10], row[9], mkt_id))
										cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[9], row[10]))
									ins_perf = True
									off_id = cursor.fetchone()[0]
							else:
								if "_AS_REQ_" in fname[-35:]:
									cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[3], row[11], row[3], mkt_id))
									cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[3], row[11]))
								elif "_AS_RESULTS_" in fname[-35:]:
									cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[6], row[4], row[6], mkt_id))
									cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[6], row[4]))
								elif "_PRC_AS_" in fname[-35:]:
									cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[7], row[4], row[7], mkt_id))
									cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[7], row[4]))
								elif "_PRC_INTVL_AS_" in fname[-35:]:
									cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[9], row[10], row[9], mkt_id))
									cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[9], row[10]))
								ins_perf = True
								off_id = cursor.fetchone()[0]
						else:
							if "_AS_REQ_" in fname[-35:]:
								off_check = [x for (x, y, z) in offer_base_rs if (row[3], row[11]) == (y, z)]
							elif "_AS_RESULTS_" in fname[-35:]:
								off_check = [x for (x, y, z) in offer_base_rs if (row[6], row[4]) == (y, z)]
							elif "_PRC_AS_" in fname[-35:]:
								off_check = [x for (x, y, z) in offer_base_rs if (row[7], row[4]) == (y, z)]
							elif "_PRC_INTVL_AS_" in fname[-35:]:
								off_check = [x for (x, y, z) in offer_base_rs if (row[9], row[10]) == (y, z)]
							if len(off_check) > 0:
								off_id = off_check[0]
								ins_perf = False
							else:
								if "_AS_REQ_" in fname[-35:]:
									cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[3], row[11], row[3], mkt_id))
									cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[3], row[11]))
								elif "_AS_RESULTS_" in fname[-35:]:
									cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[6], row[4], row[6], mkt_id))
									cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[6], row[4]))
								elif "_PRC_AS_" in fname[-35:]:
									cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[7], row[4], row[7], mkt_id))
									cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[7], row[4]))
								elif "_PRC_INTVL_AS_" in fname[-35:]:
									cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[9], row[10], row[9], mkt_id))
									cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[9], row[10]))
								ins_perf = True
								off_id = cursor.fetchone()[0]

						if 'DAM' in fname[-35:]:
							mrun_id = 'DAM'
						elif 'RTM' in fname[-35:]:
							mrun_id = 'RTM'
						elif 'HASP' in fname[-35:]:
							mrun_id = 'HASP'

						if "_AS_REQ_" in fname[-35:]:
							intv_dt = row[7].split('T')[0].split('-')[0] + '-' + row[7].split('T')[0].split('-')[1].zfill(2) + '-' + row[7].split('T')[0].split('-')[2].zfill(2)
							intv_start = intv_dt + ' ' + row[7].split('T')[1].split(':')[0].zfill(2) + ':' + row[7].split('T')[1].split(':')[1].zfill(2) + ':00'
							hr = int(row[7].split('T')[1].split(':')[0]) + 1
							if mrun_id == 'RTM':
								if int(row[7].split('T')[1].split(':')[1]) == 0:
									iv = 1
								elif int(row[7].split('T')[1].split(':')[1]) == 15:
									iv = 2
								elif int(row[7].split('T')[1].split(':')[1]) == 30:
									iv = 3
								elif int(row[7].split('T')[1].split(':')[1]) == 45:
									iv = 4
							else:
								iv = 0
						elif ("_AS_RESULTS_" in fname[-35:]) or ("_PRC_AS_" in fname[-35:]) or ("_PRC_INTVL_AS_" in fname[-35:]):
							intv_dt = row[0].split('T')[0].split('-')[0] + '-' + row[0].split('T')[0].split('-')[1].zfill(2) + '-' + row[0].split('T')[0].split('-')[2].zfill(2)
							intv_start = intv_dt + ' ' + row[0].split('T')[1].split(':')[0].zfill(2) + ':' + row[0].split('T')[1].split(':')[1].zfill(2) + ':00'
							hr = int(row[0].split('T')[1].split(':')[0]) + 1
							if mrun_id == 'RTM':
								if int(row[0].split('T')[1].split(':')[1]) == 0:
									iv = 1
								elif int(row[0].split('T')[1].split(':')[1]) == 15:
									iv = 2
								elif int(row[0].split('T')[1].split(':')[1]) == 30:
									iv = 3
								elif int(row[0].split('T')[1].split(':')[1]) == 45:
									iv = 4
							else:
								iv = 0

						intv_end = (datetime.datetime.strptime(intv_start, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours = 1, minutes = 0)).strftime("%Y-%m-%d %H:%M:%S")
						intv_id = str(off_id) + '-' + mrun_id + '-' + intv_start[2:4] + intv_start[5:7] + intv_start[8:10] + intv_start[11:13] + intv_start[14:16]

						cursor.execute("SELECT interval_id FROM interval_meta USE INDEX (PRIMARY) WHERE interval_id = %s", (intv_id,))
						intvid_rs = cursor.fetchone()
						if intvid_rs == None:
							cursor.execute("INSERT INTO interval_meta (interval_id, offer_id, market_id, mkt_run_id, interval_dt, interval_start, interval_end, opr_hour, opr_interval) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (intv_id, off_id, mkt_id, mrun_id, intv_dt, intv_start, intv_end, hr, iv))

						cursor.execute("SELECT interval_id FROM caiso_results USE INDEX (IDX_CAISO_RESULTS_INTERVAL_ID) WHERE interval_id = %s", (intv_id,))
						caiso_rs = cursor.fetchone()
						if caiso_rs == None:
							caiso_rs = []
						else:
							caiso_rs = list(caiso_rs)
						xml_item_map = {'NS_REQ_MAX_MW': 'nsreq_max', 'NS_REQ_MIN_MW': 'nsreq_min', 'RD_REQ_MAX_MW': 'rdreq_max', 'RD_REQ_MIN_MW': 'rdreq_min', 'RMD_REQ_MAX_MW': 'rmdreq_max', 'RMD_REQ_MIN_MW': 'rmdreq_min', 'RU_REQ_MAX_MW': 'rureq_max', 'RU_REQ_MIN_MW': 'rureq_min', 'RMU_REQ_MAX_MW': 'rmureq_max', 'RMU_REQ_MIN_MW': 'rmureq_min', 'SP_REQ_MAX_MW': 'spreq_max', 'SP_REQ_MIN_MW': 'spreq_min', 'NS_PROC_MW': 'nsproc_cap', 'NS_SPROC_MW': 'nsself_cap', 'NS_TOT_CST_PRC': 'nscost_line', 'NS_CLR_PRC': 'nsclr_price', 'NS_TOT_MW': 'nstot_cap', 'RD_PROC_MW': 'rdproc_cap', 'RD_SPROC_MW': 'rdself_cap', 'RD_TOT_CST_PRC': 'rdcost_line', 'RD_CLR_PRC': 'rdclr_price', 'RD_TOT_MW': 'rdtot_cap', 'RMD_PROC_MW': 'rmdproc_cap', 'RMD_SPROC_MW': 'rmdself_cap', 'RMD_TOT_CST_PRC': 'rmdcost_line', 'RMD_CLR_PRC': 'rmdclr_price', 'RMD_TOT_MW': 'rmdtot_cap', 'RU_PROC_MW': 'ruproc_cap', 'RU_SPROC_MW': 'ruself_cap', 'RU_TOT_CST_PRC': 'rucost_line', 'RU_CLR_PRC': 'ruclr_price', 'RU_TOT_MW': 'rutot_cap', 'RMU_PROC_MW': 'rmuproc_cap', 'RMU_SPROC_MW': 'rmuself_cap', 'RMU_TOT_CST_PRC': 'rmucost_line', 'RMU_CLR_PRC': 'rmuclr_price', 'RMU_TOT_MW': 'rmutot_cap', 'SP_PROC_MW': 'spproc_cap', 'SP_SPROC_MW': 'spself_cap', 'SP_TOT_CST_PRC': 'spcost_line', 'SP_CLR_PRC': 'spclr_price', 'SP_TOT_MW': 'sptot_cap'}
						if "_AS_REQ_" in fname[-35:]:
							if row[5].strip() in xml_item_map.keys():
								if len(caiso_rs) > 0:
									qry = "UPDATE caiso_results SET " + xml_item_map[row[5]] + " = %s WHERE interval_id = %s"
									cursor.execute(qry, (float(row[6]), intv_id))
								else:
									qry = "INSERT INTO caiso_results (interval_id, " + xml_item_map[row[5]] + ") VALUES (%s, %s)"
									cursor.execute(qry, (intv_id, float(row[6])))
							else:
								print (row[5].strip() + " is a new XML_DATA_ITEM for the interval with interval_id: " + intv_id)
						elif "_AS_RESULTS_" in fname[-35:]:
							if row[12].strip() in xml_item_map.keys():
								if len(caiso_rs) > 0:
									qry = "UPDATE caiso_results SET " + xml_item_map[row[12]] + " = %s WHERE interval_id = %s"
									cursor.execute(qry, (float(row[13]), intv_id))
								else:
									qry = "INSERT INTO caiso_results (interval_id, " + xml_item_map[row[12]] + ") VALUES (%s, %s)"
									cursor.execute(qry, (intv_id, float(row[13])))
							else:
								print (row[12].strip() + " is a new XML_DATA_ITEM for the interval with interval_id: " + intv_id)
						elif "_PRC_AS_" in fname[-35:]:
							if row[9].strip() in xml_item_map.keys():
								if len(caiso_rs) > 0:
									qry = "UPDATE caiso_results SET " + xml_item_map[row[9]] + " = %s WHERE interval_id = %s"
									cursor.execute(qry, (float(row[10]), intv_id))
								else:
									qry = "INSERT INTO caiso_results (interval_id, " + xml_item_map[row[9]] + ") VALUES (%s, %s)"
									cursor.execute(qry, (intv_id, float(row[10])))
							else:
								print (row[9].strip() + " is a new XML_DATA_ITEM for the interval with interval_id: " + intv_id)
						elif "_PRC_INTVL_AS_" in fname[-35:]:
							if row[6].strip() in xml_item_map.keys():
								if len(caiso_rs) > 0:
									qry = "UPDATE caiso_results SET " + xml_item_map[row[6]] + " = %s WHERE interval_id = %s"
									cursor.execute(qry, (float(row[8]), intv_id))
								else:
									qry = "INSERT INTO caiso_results (interval_id, " + xml_item_map[row[6]] + ") VALUES (%s, %s)"
									cursor.execute(qry, (intv_id, float(row[8])))
							else:
								print (row[6].strip() + " is a new XML_DATA_ITEM for the interval with interval_id: " + intv_id)
			
				cnx.commit()
				fread.close()
				i += 1
			cursor.close()
			cnx.close()
		except Exception as e:
			print (str(e))

	def __str__(self):
		try:
			if os.path.isdir(self.prog_dir + '\\cache\\caiso') == False:
				os.makedirs(self.prog_dir + '\\cache\\caiso')
			os.chdir(self.prog_dir + '\\cache\\caiso')
			if len(self.fname) > 0:
				fwrite = open('caiso-cache.txt', 'a')
				for each_file in self.fname:
					fwrite.write(each_file + '\n')
				fwrite.close()
				os.chdir(self.prog_dir)
				return (self.dt + ': CSV file(s) for group ID ' + self.params['groupid'] + ': ' + ', '.join(self.fname) + '\n')
			else:
				os.chdir(self.prog_dir)
				return (self.dt + ': No CSV file(s) for group ID: ' + self.params['groupid'] + '\n')
		except Exception as e:
			print (str(e))


def init_cache(prog_dir):
	try:
		if os.path.isdir(prog_dir + '\\cache\\caiso') == False:
			os.makedirs(prog_dir + '\\cache\\caiso')
		os.chdir(prog_dir + '\\cache\\caiso')
		fwrite = open('caiso-cache.txt', 'w')
		fwrite.write('File(s) cached are as follows:\n')
		fwrite.close()
		os.chdir(prog_dir)
		return "The cache file has been initialized successfully.\n"
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
		if start_dt == (dbdt_end + datetime.timedelta(hours = 0, minutes = 0)) and end_dt >= start_dt and end_dt <= datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(hours = 24, minutes = 0), "%Y-%m-%d"), "%Y-%m-%d"):
			print ("\nThe database date validation check has completed successfully. The program will now execute...\n")
			return True
		else:
			actual_st = datetime.datetime.strftime(dbdt_end + datetime.timedelta(hours = 0, minutes = 0), "%Y-%m-%d")
			actual_ed = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(hours = 24, minutes = 0), "%Y-%m-%d")
			print ("\nPlease check the script start and end dates properly. The start date must be set to " + actual_st.split('-')[1] + '-' + actual_st.split('-')[2] + '-' + actual_st.split('-')[0] + " (MM-DD-YYYY) and the end date must be less than or equal to " + actual_ed.split('-')[1] + '-' + actual_ed.split('-')[2] + '-' + actual_ed.split('-')[0] + " (MM-DD-YYYY) and also not less than the start date.")
			return False
	except Exception as e:
		print (str(e))


def main():
	print ("\n********** Start of the Program **********\n")

	base_url = "http://oasis.caiso.com/oasisapi/GroupZip?"
	groupname = ['DAM_PRC_AS_GRP', 'HASP_PRC_AS_GRP', 'RTM_PRC_AS_GRP', 'DAM_AS_GRP', 'HASP_AS_GRP', 'RTM_AS_GRP', 'PUB_DAM_GRP', 'PUB_RTM_GRP']
	
	# prog_dir is the main directory under which the CSV files will be stored
	#prog_dir = "C:\\Users\\Omkar Sunkersett\\Downloads\\markets"

	# This command initializes the cache file once the prog_dir variable has been set
	#print (init_cache(prog_dir))

	# These respective variables set the start and end dates for fetching data from the server
	#startdatetime = "MM-DD-YYYY"
	#enddatetime = "MM-DD-YYYY"

	if dbdt_check("CAISO", startdatetime, enddatetime):
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
					caiso_dt = str(year) + str(month).zfill(2) + str(day).zfill(2) + "T07:00-0000"
					streams_data = [Caiso(gname, caiso_dt, prog_dir) for gname in groupname]
					for each_stream in streams_data:
						# Code for fetching the CSV files from the server for all markets
						#each_stream.fetch_files(base_url, '\\'+str(year)+'\\'+str(month).zfill(2)+'\\'+str(day).zfill(2))
						#print (each_stream)
						#time.sleep(6)
						pass
				start_day = 1
			start_month = 1

		# Code for loading the CSV data into the not-published database for all markets
		# IMPORTANT: Make sure you have the latest backup of the database before uncommenting the below lines
		#print ("\nLoading the new data into the database...\n")
		#load_db = Caiso('', '', prog_dir)
		#load_db.etl_file_data(prog_dir + "\\cache\\caiso\\caiso-cache.txt")

	print ("\n********** End of the Program **********\n")


#main()
