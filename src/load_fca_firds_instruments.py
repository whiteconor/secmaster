
import xml.sax
import argparse
import datetime
import io
import json
import os
import requests
import zipfile
import logging
import pandas as pd
from typing import List, Dict

from firds.firds_xml_handler import FIRDSHandler
from firds.firds_query import search_firds_files

from misc.lse import get_lse_isin_to_currency

from lib.openfigi import OpenFIGI
from libsecdb import SECDB

logger = logging.getLogger( 'fca_firds_download' )
logger.setLevel( logging.INFO )
log_formatter = logging.Formatter( '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
log_handler = logging.StreamHandler()
log_handler.setFormatter( log_formatter )
logger.addHandler( log_handler )


def get_firds_dataframe( file_list: List[Dict], file_type: str ) -> pd.DataFrame:
    
	handler = FIRDSHandler()
	parser = xml.sax.make_parser()
	parser.setContentHandler( handler )
    
	for insts_file in file_list:
		process_file = False

		if insts_file['_source']['file_type'] == file_type:

			category = insts_file['_source']['file_name'][7:8]
			if category in ('C','E'):
				#Only process equity and etf type products
				process_file = True

			if process_file:
				download_file = insts_file['_source']['download_link']
				logger.info( f'Downloading file {download_file}' )
				res = requests.get( download_file )

				if res.status_code == 200:
					archive = zipfile.ZipFile( io.BytesIO( res.content ) )

					for compressed_file in archive.filelist:
						logger.info( f'Processing file {compressed_file.filename}' )
						parser.parse( archive.open( compressed_file.filename, 'r' ) )
				else:
					logger.error( f'Failed download file {download_file} : {res.text}')
			else:
				logger.debug( f'Skipping file {insts_file["_source"]["file_name"]}' )

		inst_data = []
    
		for instrument in handler.instruments():
			cfi_code = instrument['FinInstrmGnlAttrbts']['ClssfctnTp']
			isin = instrument['FinInstrmGnlAttrbts']['Id']

			try:
				name = instrument['FinInstrmGnlAttrbts']['FullNm']
			except KeyError:
				logger.error( f'No name for security {isin}')
				name = ''

			currency = instrument['FinInstrmGnlAttrbts']['NtnlCcy']
			issuer_lei = instrument['Issr']
   
			try:
				trading_venue = instrument['TradgVnRltdAttrbts']['Id']
			except KeyError:
				trading_venue = ''

			try:
				reference_isin = instrument['DerivInstrmAttrbts']['UndrlygInstrm']['Sngl']['ISIN']
			except KeyError:
				reference_isin = ''
	
			inst_data.append(
				{
					'isin' : isin,
					'currency' : currency,
					'cfi_code' : cfi_code,
					'trading_venue' : trading_venue,
					'issuer_lei' : issuer_lei,
					'reference_isin' : reference_isin
				}
			)
 
	insts_df = pd.DataFrame( inst_data )
	return insts_df

exchange_codes =         ['GA','AU','AV','BB','SW','CN','CH','DC','FH','FP','GR','HK','ID','IM','IN','IT','JP','SS','KS','LN','LX','MM','NA','NO','NZ','PL','PW','SJ','SM','SP','TT','VX', 'US']
reuters_exchange_codes = ['AT','AX','VI','BR','SW','TO','SS','CO','HE','PA','DE','HK','IR','MI','NS','TA','T','ST','KS', 'L', 'LU','MX','AS','OL','NZ','LS','WA','JO','MC','SI','TW','VX', '']
security_types = ['Common Stock','ETP','ADR','GDR']
secdb_loc = '/data/refdata.db'

bbg_exch_codes_to_reuters = dict( zip( exchange_codes, reuters_exchange_codes ) )

def open_figi_query( row ):
    return { 'idType' : 'ID_ISIN', 'idValue' : row['isin'], 'marketSecDes' : 'Equity', 'currency' : row['currency'] }

def open_figi_output( req, item ):
    return {
        'vendor_symbol' : req['idValue'],
        'symbol' : item['ticker'],
        'exch_code' : item['exchCode'],
        'name' : item['name'],
        'figi' : item['figi'],
        'composite_figi' : item['compositeFIGI'],
        'share_class_figi' : item['shareClassFIGI'],
        'security_type' : item['securityType'],
        'security_type2' : item['securityType2'],
        'currency' : req['currency']
    }
    
def open_figi_filter( item ):
    return item['exchCode'] in exchange_codes and item['securityType'] in security_types

def add_instruments( instrument_adds: pd.DataFrame, secdb: SECDB ):
    
	if len(instrument_adds) == 0:
		logger.info( 'No instruments to add' )
	else:
    
		fin_types = secdb.get_fin_types()
		trading_places = secdb.get_trading_places()
		currencies = secdb.get_currencies()
		instruments_all = secdb.get_live_instruments_raw()
		
		instrument_adds['financial_type_id'] = instrument_adds['security_type'].map( fin_types.reset_index().set_index('financial_type')['financial_type_id'] )
		instrument_adds['country_id'] = instrument_adds['exch_code'].map( trading_places.reset_index().set_index('composite_exchange_code')['country_id'].drop_duplicates() )
		instrument_adds['currency_id'] = instrument_adds['currency'].map( currencies.reset_index().set_index('iso_code')['currency_id'] )
		instrument_adds['fsymbol'] = instrument_adds['symbol'] + '-' + instrument_adds['exch_code']
		instrument_adds['status_code'] = 'A'
		instrument_adds['issuer_id'] = 0
		instrument_adds['version'] = 1
		instrument_adds['timestamp'] = datetime.datetime.now().strftime( '%Y-%m-%d %H:%M:%S' )

		output_columns = [
			'symbol',
			'fsymbol',
			'name',
			'figi',
			'share_class_figi',
			'financial_type_id',
			'country_id',
			'currency_id',
			'status_code',
			'issuer_id',
			'version',
			'timestamp'
		]

		output = instrument_adds[ output_columns ].copy()
		output.rename( columns = { 'symbol' : 'ticker' }, inplace=True )
	
		if len(instruments_all)	> 0:
			max_id = max(instruments_all['instrument_id'])
		else:
			max_id = 0

		output.index = list(range(max_id + 1, max_id + 1 + len(output) ))

		for symbol in output['ticker'].values:
			logger.debug( f'Adding new symbol {symbol}' )
		
		output.to_sql( 'instruments', secdb.dbh, if_exists='append', index_label='instrument_id' )
		logger.info( f'Inserted {len(output)} symbols to instruments table' )
  
		country_to_exchange_code = trading_places[['country_id','composite_exchange_code']].drop_duplicates().set_index('country_id')['composite_exchange_code']

		#Add related Bloomberg tickers for new symbols
		bb_tickers = pd.DataFrame( index=output.index )
		bb_tickers['vendor_symbol'] = output['ticker'] + ' ' + output['country_id'].map( country_to_exchange_code ) + ' Equity'
		bb_tickers['vendor_code_id'] = 4
		bb_tickers['trading_place_id'] = 0
		bb_tickers['effective_date'] = datetime.datetime.today().date().strftime( '%Y-%m-%d' )
		bb_tickers['end_date'] = None
		bb_tickers['vendor_symbol_source_id'] = 6
		bb_tickers = bb_tickers.reset_index()
		bb_tickers.columns = ['instrument_id','vendor_symbol','vendor_code_id','trading_place_id','effective_date','end_date','vendor_symbol_source_id']
		bb_tickers.to_sql( 'vendor_mappings', secdb.dbh, if_exists='append', index=False )

		logger.info( f'Inserted {len(bb_tickers)} Bloomberg tickers to vendor mappings table' )

		#Add related Reuters/Refinitiv codes for new symbols
		reuters_code = pd.DataFrame( index=output.index )
		reuters_code['vendor_symbol'] = output['ticker'].str.replace('/','-') + '.' + output['country_id'].map( country_to_exchange_code ).map(bbg_exch_codes_to_reuters)
		reuters_code['vendor_code_id'] = 5
		reuters_code['trading_place_id'] = 0
		reuters_code['effective_date'] = datetime.datetime.today().date().strftime( '%Y-%m-%d' )
		reuters_code['end_date'] = None
		reuters_code['vendor_symbol_source_id'] = 9
		reuters_code = reuters_code.reset_index()
		reuters_code.columns = ['instrument_id','vendor_symbol','vendor_code_id','trading_place_id','effective_date','end_date','vendor_symbol_source_id']
		reuters_code.to_sql( 'vendor_mappings', secdb.dbh, if_exists='append', index=False )

		logger.info( f'Inserted {len(reuters_code)} Reuters/Refinitiv codes to vendor mappings table' )
  
		#Add ISIN codes for new symbols - Need to ensure indexes align
		tmp_isin_codes = instrument_adds['vendor_symbol']
		tmp_isin_codes.index = output.index

		isin_codes = pd.DataFrame( index=output.index )
		isin_codes['vendor_symbol'] = tmp_isin_codes
		isin_codes['vendor_code_id'] = 7
		isin_codes['trading_place_id'] = 0
		isin_codes['effective_date'] = datetime.datetime.today().date().strftime( '%Y-%m-%d' )
		isin_codes['end_date'] = None
		isin_codes['vendor_symbol_source_id'] = 9
		isin_codes = isin_codes.reset_index()
		isin_codes.columns = ['instrument_id','vendor_symbol','vendor_code_id','trading_place_id','effective_date','end_date','vendor_symbol_source_id']
		isin_codes.to_sql( 'vendor_mappings', secdb.dbh, if_exists='append', index=False )
  
		logger.info( f'Inserted {len(isin_codes)} ISIN codes to vendor mappings table' )

def delete_instruments( instrument_dels: pd.DataFrame, secdb: SECDB ):
    
	if len(instrument_dels) == 0:
		logger.info( 'No instruments to delete' )
	else:
		instrument_dels['status_code'] = 'D'
		instrument_dels['version'] += 1
		instrument_dels['timestamp'] = datetime.datetime.now().strftime( '%Y-%m-%d %H:%M:%S' )

		for symbol in instrument_dels['ticker'].values:
			logger.info( f'Deactivating symbol {symbol}' )

		instrument_dels.to_sql( 'instruments', secdb.dbh, if_exists='append', index=False )
		logger.info( f'Deactivated {len(instrument_dels)} symbols in the intruments table' )

		#Expire and vendor mappings for deleted symbols
		instrument_ids = ','.join( [str(id) for id in instrument_dels['instrument_id'].values] )
		update_sql = f'update vendor_mappings set end_date = datetime() where instrument_id in ({instrument_ids}) and end_date is null'
		cursor = secdb.dbh.cursor()
		cursor.execute(update_sql)

def process_ticker_changes( ticker_changes: pd.DataFrame, secdb: SECDB ):
    
	if len(ticker_changes) == 0:
		logger.info( 'No ticker changes to process' )
	else:
		instruments_all = secdb.get_live_instruments_raw()
		trading_places = secdb.get_trading_places()
		country_to_exchange_code = trading_places[['country_id','composite_exchange_code']].drop_duplicates().set_index('country_id')['composite_exchange_code']
	
		change_inserts = instruments_all[instruments_all['figi'].isin(ticker_changes.index)].copy()
		change_inserts['version'] += 1 
		change_inserts['ticker'] = change_inserts['figi'].map( ticker_changes['symbol'] )
		change_inserts['fsymbol'] = change_inserts['ticker'] + '-' + change_inserts['country_id'].map( country_to_exchange_code )
		change_inserts['name'] = change_inserts['figi'].map( ticker_changes['name_y'] )
		change_inserts['timestamp'] = datetime.datetime.now()

		for idx, ticker_change in ticker_changes.iterrows():
			old_symbol = ticker_change['ticker']
			new_symbol = ticker_change['symbol']
			logger.info( f'Changing ticker symbol {old_symbol} to {new_symbol}' )
		
		change_inserts.to_sql( 'instruments', secdb.dbh, if_exists='append', index=False )
		logger.info( f'{len(change_inserts)} symbols renamed in instruments table' )

		#Update Related Bloomberg Tickers
		instrument_ids = ','.join( [str(id) for id in change_inserts['instrument_id'].values] )
		sql = f'select * from vendor_mappings where vendor_code_id = 4 and instrument_id in ({instrument_ids}) and end_date is null'
		bb_tickers = pd.read_sql( sql, secdb.dbh )

		update_sql = f'update vendor_mappings set end_date = datetime() where vendor_code_id = 4 and instrument_id in ({instrument_ids}) and end_date is null'
		cursor = secdb.dbh.cursor()
		cursor.execute(update_sql)

		bb_tickers['vendor_symbol'] = bb_tickers['instrument_id'].map( change_inserts.set_index('instrument_id')['ticker'] ) + ' ' + bb_tickers['instrument_id'].map(change_inserts.set_index('instrument_id')['country_id']).map( country_to_exchange_code ) + ' Equity'
		bb_tickers['effective_date'] = datetime.datetime.today().date().strftime( '%Y-%m-%d' )
		bb_tickers.to_sql( 'vendor_mappings', secdb.dbh, if_exists='append', index=False )
		logger.info( f'Renamed {len(bb_tickers)} Bloomberg tickers in vendor mappings table' )

		#Update Related Reuters symbols
		instrument_ids = ','.join( [str(id) for id in change_inserts['instrument_id'].values] )
		sql = f'select * from vendor_mappings where vendor_code_id = 10 and instrument_id in ({instrument_ids}) and end_date is null'
		reuters_codes = pd.read_sql( sql, secdb.dbh )

		update_sql = f'update vendor_mappings set end_date = datetime() where vendor_code_id = 10 and instrument_id in ({instrument_ids}) and end_date is null'
		cursor = secdb.dbh.cursor()
		cursor.execute(update_sql)

		reuters_codes['vendor_symbol'] = reuters_codes['instrument_id'].map( change_inserts.set_index('instrument_id')['ticker'] ).str.replace('/','-')  + '.' + change_inserts['country_id'].map( country_to_exchange_code ).map(bbg_exch_codes_to_reuters)
		reuters_codes['effective_date'] = datetime.datetime.today().date().strftime( '%Y-%m-%d' )
		reuters_codes.to_sql( 'vendor_mappings', secdb.dbh, if_exists='append', index=False )
		logger.info( f'Renamed {len(reuters_codes)} Reuters codes in vendor mappings table' )

def main():

	parser = argparse.ArgumentParser()
	parser.add_argument('--date', default = datetime.date.today().strftime( '%Y-%m-%d' ) )
	parser.add_argument('--type', choices=['FULINS','DLTINS'], required=True )
	args = parser.parse_args()

	pub_date = datetime.datetime.strptime( args.date, '%Y-%m-%d' )
	file_type = args.type

	file_list = search_firds_files( pub_date, file_type )
	print(f'Found {len(file_list)} FIRDS files')
	firds_instruments = get_firds_dataframe( file_list, file_type )
	firds_instruments_reduced = firds_instruments[['isin','currency']].drop_duplicates()

	#For LSE need to figure out which ISINs are traded in pence or pounds
	lse_isin_to_currency = get_lse_isin_to_currency()
	firds_instruments_reduced['currency'] = firds_instruments_reduced['isin'].map( lse_isin_to_currency ).fillna(firds_instruments_reduced['currency'])
  
	isin_to_lei = firds_instruments[['isin','issuer_lei']].drop_duplicates().set_index('isin')
 
	openfigi = OpenFIGI( logger )
	firds_mapped, exceptions = openfigi.map_symbols( firds_instruments_reduced, open_figi_query, open_figi_output, open_figi_filter )

 
	#firds_mapped = pd.read_csv( '/data/fca_firds_openfigi_mapped.csv' )
	secdb = SECDB( logger, secdb_loc )
	instruments_all = secdb.get_live_instruments_raw()

	instrument_adds = firds_mapped[firds_mapped['figi'].isin(instruments_all['figi']) == False].copy()
	logger.info( f'{len(instrument_adds)} new instruments' )
 
	add_instruments( instrument_adds, secdb )
 
	instrument_dels = instruments_all[instruments_all['figi'].isin(firds_mapped['figi']) == False].copy()
	logger.info( f'{len(instrument_dels)} instrument deletes' )
 
	delete_instruments( instrument_dels, secdb )
 
	compare = pd.merge( instruments_all[['figi','ticker','name']], firds_mapped[['figi','symbol','name']], left_on='figi', right_on='figi', how='inner' )
	ticker_changes = compare[compare['ticker'] != compare['symbol']].set_index('figi')
	logger.info( f'{len(ticker_changes)} ticker changes' )
 
	process_ticker_changes( ticker_changes, secdb )

if __name__ == '__main__':
	main()
