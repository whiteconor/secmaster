import pandas as pd
import logging
import datetime
import argparse

from lib.secdb import SECDB
from lib.openfigi import OpenFIGI

secdb_loc = '/data/refdata.db'
openfigi_api_key = '53e5a556-fb8a-4a8e-a173-e11aea0fd046'

def open_figi_query( row ):
	return { 'idType' : 'ID_ISIN', 'idValue' : row['isin'], 'exchCode' : 'US', 'marketSecDes' : 'Equity', 'includeUnlistedEquities' : True }

def open_figi_output( req, item ):
	return {
		'vendor_symbol' : req['idValue'],
		'symbol' : item['ticker'],
		'exch_code' : item['exchCode'],
		'name' : item['name'],
		'figi' : item['figi'],
		'composite_figi' : item['compositeFIGI'],
		'share_class_figi' : item['shareClassFIGI'],
		'security_type' : item['securityType']
	}

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('--date', default = datetime.date.today().strftime( '%Y-%m-%d' ) )
	args = parser.parse_args()
	run_date = args.date
 
	logger = logging.getLogger( 'update_gleif_isins' )
	logger.setLevel( logging.DEBUG )
	log_formatter = logging.Formatter( '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
	log_handler = logging.StreamHandler()
	log_handler.setFormatter( log_formatter )
	logger.addHandler( log_handler )
 
	secdb = SECDB( logger, secdb_loc )
	openfigi = OpenFIGI( openfigi_api_key, logger )

	logger.info( f'Querying for new ISIN codes on date {run_date}' )
	isin_to_lei = secdb.get_isin_to_leis_by_date( run_date ).reset_index()

	if len(isin_to_lei) > 0:
		logger.info( f'Loaded {len(isin_to_lei)} new ISIN codes' )
		figi_mapped, exceptions = openfigi.map_symbols( isin_to_lei, open_figi_query, open_figi_output )

		if len(figi_mapped) > 0:
			isin_update_list = figi_mapped[['figi','vendor_symbol']].copy()
			secdb.update_vendor_symbols( isin_update_list, vendor_code='ISIN', source='GLEIF-ISIN-LEI-MAP' )
		else:
			logger.warn( f'New new ISIN codes found for date {run_date}' )
	else:
		logger.warn( f'New new ISIN codes found for date {run_date}' )

if __name__ == '__main__':
	main()
