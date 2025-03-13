import requests
import pandas as pd
import datetime
import logging
from bs4 import BeautifulSoup
from lib.secdb import SECDB

secdb_loc = '/data/refdata.db'

def get_latest_download_link() -> str:
	url = url = 'https://www.gleif.org/en/lei-data/lei-mapping/download-isin-to-lei-relationship-files'

	res = requests.get( url )
	soup = BeautifulSoup( res.text, 'html.parser' )
 
	rows = []

	for tr in soup.find('table').find_all("tr")[1:]:  # Skip header row
		cols = tr.find_all("td")
		date = cols[0].text.strip()
		link_tag = cols[1].find("a")
		filename = link_tag.text.strip()
		link = link_tag["href"] if link_tag else ""
		rows.append([date, filename, link])

	# Create DataFrame
	df = pd.DataFrame(rows, columns=["Date", "Filename", "Link"])
	return df.loc[0]['Link']

def main():
	logger = logging.getLogger( 'isin_to_lei_update' )
	logger.setLevel( logging.DEBUG )
	log_formatter = logging.Formatter( '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
	log_handler = logging.StreamHandler()
	log_handler.setFormatter( log_formatter )
	logger.addHandler( log_handler )
 
	secdb = SECDB( logger, secdb_loc )

	logger.info( 'Loading isin_to_lei table into memory' )
	isin_to_lei = secdb.get_isin_to_leis()
	logger.info( f'Loaded {len(isin_to_lei)} rows' )

	download_link = get_latest_download_link()
	logger.info( f'Loading latest data from {download_link}' )
	latest_file = pd.read_csv( download_link, compression='zip' )

	additions = latest_file[latest_file['ISIN'].isin(isin_to_lei.index) == False]
	deletions = isin_to_lei[isin_to_lei.index.isin(latest_file['ISIN'])==False].reset_index()

	#Insert new mappings to table
	logger.info( f'Insert {len(additions)} new mappings to table' )
	insert_sql = 'insert into isin_to_lei values (?,?,?,?,?)'
	created = datetime.date.today().strftime( '%Y-%m-%d' )
	ts = datetime.datetime.today().strftime( '%Y-%m-%d %H:%M:%S' )
	cursor = secdb.dbh.cursor()
	_ = additions.apply( lambda row: cursor.execute( insert_sql, ( row['ISIN'], row['LEI'], 'Active', created, ts ) ), axis=1 )
	secdb.dbh.commit()

	#Set status to Inactive for mappings no longer present in the daily file
	logger.info( f'Set status to Inactive for {len(deletions)} mappings no longer present in the daily file' )
	update_sql = "update isin_to_lei set status = 'Inactive', last_updated = ? where isin = ?"
	cursor = secdb.dbh.cursor()
	_ = deletions.apply( lambda row: cursor.execute( update_sql, ( ts, row['isin'] ) ), axis=1 )
	secdb.dbh.commit()

	logger.info( 'Update last_updated for all securities still Active' )
	update_sql_dt = "update isin_to_lei set last_updated = ? where status = 'Active'"
	cursor = secdb.dbh.cursor()
	cursor.execute( update_sql_dt, (ts,) )
	secdb.dbh.commit()

if __name__ == '__main__':
	main()
