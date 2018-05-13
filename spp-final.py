# Author: Omkar Sunkersett
# Purpose: To fetch SPP data and update the database
# Summer Internship at Argonne National Laboratory

import csv, datetime, ftplib, MySQLdb, os, time

class SPP():
	def __init__(self, server, path, start_dt, end_dt, prog_dir):
		self.files_cached = []
		try:
			self.ftp_handle = ftplib.FTP(server)
			self.ftp_handle.login()
			self.path_name = path
			self.start_dt = datetime.datetime.strptime(start_dt, "%m-%d-%Y")
			self.end_dt = datetime.datetime.strptime(end_dt, "%m-%d-%Y")
			self.prog_dir = prog_dir
		except Exception as e:
			print (str(e))

	def fetch_files(self, pres_wd, dir_wd):
		try:
			try:
				self.ftp_handle.voidcmd("NOOP")
			except Exception as e:
				print (str(e))
				self.ftp_handle = ftplib.FTP("pubftp.spp.org")
				self.ftp_handle.login()
			self.ftp_handle.cwd(pres_wd.replace('\\', '/') + '/' + dir_wd)
			dir_lst = [x for x in self.ftp_handle.nlst() if '.' not in x]
			if dir_lst == []:
				files_lst = [x for x in self.ftp_handle.nlst() if '.' in x and x.split('-')[1] == 'OR' and datetime.datetime.strptime(x.split('-')[3][:8], "%Y%m%d") >= self.start_dt and datetime.datetime.strptime(x.split('-')[3][:8], "%Y%m%d") <= self.end_dt]
				if len(files_lst) > 0:
					if os.path.isdir(self.prog_dir + '\\cache\\spp' + pres_wd + '\\' + dir_wd) == False:
						os.makedirs(self.prog_dir + '\\cache\\spp' + pres_wd + '\\' + dir_wd)
					os.chdir(self.prog_dir + '\\cache\\spp' + pres_wd + '\\' + dir_wd)
					for file_name in files_lst:
						print (os.getcwd() + '\\' + file_name)
						self.ftp_handle.retrbinary("RETR " + file_name, open(file_name, 'wb').write)
						self.files_cached.append(os.getcwd() + '\\' + file_name)
					os.chdir(self.prog_dir + '\\cache\\spp' + pres_wd)
				self.ftp_handle.cwd('..')
			else:
				files_lst = [x for x in self.ftp_handle.nlst() if '.' in x and x.split('-')[1] == 'OR' and datetime.datetime.strptime(x.split('-')[3][:8], "%Y%m%d") >= self.start_dt and datetime.datetime.strptime(x.split('-')[3][:8], "%Y%m%d") <= self.end_dt]
				if len(files_lst) > 0:
					if os.path.isdir(self.prog_dir + '\\cache\\spp' + pres_wd + '\\' + dir_wd) == False:
						os.makedirs(self.prog_dir + '\\cache\\spp' + pres_wd + '\\' + dir_wd)
					os.chdir(self.prog_dir + '\\cache\\spp' + pres_wd + '\\' + dir_wd)
					for file_name in files_lst:
						print (os.getcwd() + '\\' + file_name)
						self.ftp_handle.retrbinary("RETR " + file_name, open(file_name, 'wb').write)
						self.files_cached.append(os.getcwd() + '\\' + file_name)
				for each_dir in dir_lst:
					self.fetch_files(self.ftp_handle.pwd().replace('/', '\\'), each_dir)
				self.ftp_handle.cwd('..')
		except Exception as e:
			print (str(e))

	def __str__(self):
		try:
			self.ftp_handle.quit()
			os.chdir(self.prog_dir + '\\cache\\spp')
			fwrite = open(self.path_name[1:-1].replace('\\', '-') + '.txt', 'w')
			fwrite.write('File(s) cached are as follows:\n')
			for file_name in self.files_cached:
				fwrite.write(file_name + '\n')
			fwrite.close()
			os.chdir(self.prog_dir)
			return ("\nFile(s) cached: " + ', '.join(self.files_cached) + '\n')
		except Exception as e:
			print (str(e))


def etl_file_data(cache_file):
	try:
		fread = open(cache_file, 'r')
		flines = [x.rstrip('\n') for x in fread.readlines() if x.endswith('.csv\n')]
		fread.close()
		cnx = MySQLdb.connect(user = 'not-published', passwd = 'not-published', host = 'not-published', db = 'not-published')
		cursor = cnx.cursor()
		cursor.execute("SELECT market_id FROM market_meta USE INDEX (PRIMARY) WHERE market_name = 'SPP'")
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
				if len(row) > 0 and row[2].strip() != '' and row[3].strip() != '' and row[4].strip() != '':
					if ins_perf == True:
						cursor.execute("SELECT offer_id, identifier_1, identifier_2 FROM offer_base USE INDEX (IDX_OFFER_BASE_MARKET_ID) WHERE market_id = %s", (mkt_id,))
						offer_base_rs = list(cursor.fetchall())
						if len(offer_base_rs) > 0:
							off_check = [x for (x, y, z) in offer_base_rs if (row[2], '0') == (y, z)]
							if len(off_check) > 0:
								off_id = off_check[0]
								ins_perf = False
							else:
								cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[2], '0', "SPP", mkt_id))
								ins_perf = True
								cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[2], '0'))
								off_id = cursor.fetchone()[0]
						else:
							cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[2], '0', "SPP", mkt_id))
							ins_perf = True
							cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[2], '0'))
							off_id = cursor.fetchone()[0]
					else:
						off_check = [x for (x, y, z) in offer_base_rs if (row[2], '0') == (y, z)]
						if len(off_check) > 0:
							off_id = off_check[0]
							ins_perf = False
						else:
							cursor.execute("INSERT INTO offer_base (identifier_1, identifier_2, region_name, market_id) VALUES (%s, %s, %s, %s)", (row[2], '0', "SPP", mkt_id))
							ins_perf = True
							cursor.execute("SELECT offer_id FROM offer_base USE INDEX (IDX_OFFER_BASE_ID1_ID2) WHERE identifier_1 = %s AND identifier_2 = %s", (row[2], '0'))
							off_id = cursor.fetchone()[0]

					if fname.split('\\')[-1].split('-')[0].lower() == 'da':
						mrun_id = 'DAM'
					elif fname.split('\\')[-1].split('-')[0].lower() == 'rtbm':
						mrun_id = 'RTBM'

					intv_start = (datetime.datetime.strptime(row[0], "%m/%d/%Y %H:%M:%S") - datetime.timedelta(hours = 1, minutes = 0)).strftime("%Y-%m-%d %H:%M:%S")
					intv_end = (datetime.datetime.strptime(row[0], "%m/%d/%Y %H:%M:%S")).strftime("%Y-%m-%d %H:%M:%S")
					intv_dt = intv_start[:10]
					hr, iv = int(intv_start[11:13]), 0
					intv_id = str(off_id) + '-' + mrun_id + '-' + intv_start[2:4] + intv_start[5:7] + intv_start[8:10] + intv_start[11:13] + intv_start[14:16]

					cursor.execute("SELECT interval_id FROM interval_meta USE INDEX (PRIMARY) WHERE interval_id = %s", (intv_id,))
					intvid_rs = cursor.fetchone()
					if intvid_rs == None:
						cursor.execute("INSERT INTO interval_meta (interval_id, offer_id, market_id, mkt_run_id, interval_dt, interval_start, interval_end, opr_hour, opr_interval) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (intv_id, off_id, mkt_id, mrun_id, intv_dt, intv_start, intv_end, hr, iv))

					cursor.execute("SELECT interval_id FROM spp_results USE INDEX (IDX_SPP_RESULTS_INTERVAL_ID) WHERE interval_id = %s", (intv_id,))
					spp_rs = cursor.fetchone()
					if spp_rs == None:
						spp_rs = []
					else:
						spp_rs = list(spp_rs)
					xml_item_map = {'Capability Offer Reg-Down': 'coreg_down', 'Capability Offer Reg-Up': 'coreg_up', 'Mileage Factor Reg-Down': 'mfreg_down', 'Mileage Factor Reg-Up': 'mfreg_up', 'Mileage Offer Reg-Down': 'moreg_down', 'Mileage Offer Reg-Up': 'moreg_up', 'SPIN': 'spin_price', 'SUPP': 'supp_price'}
					if row[3].strip() in xml_item_map.keys():
						if len(spp_rs) > 0:
							qry = "UPDATE spp_results SET " + xml_item_map[row[3].strip()] + " = %s WHERE interval_id = %s"
							cursor.execute(qry, (float(row[4].strip()), intv_id))
						else:
							qry = "INSERT INTO spp_results (interval_id, " + xml_item_map[row[3].strip()] + ") VALUES (%s, %s)"
							cursor.execute(qry, (intv_id, float(row[4])))
					else:
						print (row[3].strip() + " is a new ASProduct for the interval with interval_id: " + intv_id)

			cnx.commit()
			fread.close()
			i += 1
		cursor.close()
		cnx.close()
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

	if dbdt_check("SPP", startdatetime, enddatetime):
		# Code for fetching the CSV files from the server for historical offers
		#histoff_or = SPP("pubftp.spp.org", "/Markets/HistoricalOffers/", startdatetime, enddatetime, prog_dir)
		#histoff_or.fetch_files("/Markets/HistoricalOffers", "")
		#rint(histoff_or)

		# Code for loading the historical offer related CSV data into the not-published database for OR only
		# IMPORTANT: Make sure you have the latest backup of the database before uncommenting the below lines
		#print ("\nLoading the new data into the database...\n")
		#etl_file_data(prog_dir + "\\cache\\spp\\Markets\HistoricalOffers.txt")

	print ("\n********** End of the Program **********\n")


main()
