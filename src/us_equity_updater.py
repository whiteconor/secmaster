import pandas as pd
import datetime
from lib.secdb import SECDB
import logging

secdb_loc = '/data/refdata.db'

def main():
    
	logger = logging.getLogger( 'us_equity_updater' )
	logger.setLevel( logging.DEBUG )
	log_formatter = logging.Formatter( '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
	log_handler = logging.StreamHandler()
	log_handler.setFormatter( log_formatter )
	logger.addHandler( log_handler )
 
	run_date = datetime.date.today().strftime( '%Y%m%d' )
	us_listed_file = f'/data/us_equities_figi_mapped_{run_date}.csv'

	secdb = SECDB( logger, secdb_loc )

	instruments_all = secdb.get_live_instruments_raw()

	#US only
	instruments = instruments_all[instruments_all.country_id==237]
	#Exclude Indices
	instruments = instruments_all[instruments_all.financial_type_id!=31]

	fin_types = secdb.get_fin_types()
	trading_places = secdb.get_trading_places()

	us_listed_eq = pd.read_csv( us_listed_file )
	#us_otc_eq = pd.read_csv( us_otc_file )
	#us_all_eq = pd.concat( [us_listed_eq, us_otc_eq], ignore_index=True )
	us_all_eq = us_listed_eq

	symbol_adds = us_all_eq[us_all_eq['figi'].isin(instruments['figi']) == False].copy()
	logger.info( f'{len(symbol_adds)} new symbols' )

	symbol_dels = instruments[instruments['figi'].isin(us_all_eq['figi']) == False].copy()
	logger.info( f'{len(symbol_dels)} symbol deletes' )

	compare = pd.merge( instruments[['figi','ticker','name']], us_all_eq[['figi','symbol','name']], left_on='figi', right_on='figi', how='inner' )
	ticker_changes = compare[compare['ticker'] != compare['symbol']].set_index('figi')
	logger.info( f'{len(ticker_changes)} ticker changes' )

	if len(symbol_adds) > 0:
		#Processing symbol adds
		symbol_adds['financial_type_id'] = symbol_adds['security_type'].map( fin_types.reset_index().set_index('financial_type')['financial_type_id'] )
		symbol_adds['primary_listing_id'] = symbol_adds['primary_exch_code'].map( trading_places.reset_index().set_index('exchange_code')['trading_place_id'] )
		symbol_adds['country_id'] = 237
		symbol_adds['currency_id'] = 5
		symbol_adds['fsymbol'] = symbol_adds['symbol'] + '-US'
		symbol_adds['status_code'] = 'A'
		symbol_adds['issuer_id'] = 0
		symbol_adds['version'] = 1
		symbol_adds['timestamp'] = datetime.datetime.now()

		output_columns = [
			'symbol',
			'fsymbol',
			'name',
			'figi',
			'share_class_figi',
			'financial_type_id',
			'primary_listing_id',
			'country_id',
			'currency_id',
			'status_code',
			'issuer_id',
			'version',
			'timestamp'
		]

		output = symbol_adds[ output_columns ].copy()
		output.rename( columns = { 'symbol' : 'ticker' }, inplace=True )

		output.index = list(range(max(instruments_all['instrument_id']) + 1, max(instruments_all['instrument_id']) + 1 + len(output) ))

		for symbol in output['ticker'].values:
			logger.info( f'Adding new symbol {symbol}' )
		
		output.to_sql( 'instruments', secdb.dbh, if_exists='append', index_label='instrument_id' )
		logger.info( f'Inserted {len(output)} symbols to instruments table' )

		#Add related Bloomberg tickers for new symbols
		bb_tickers = pd.DataFrame( index=output.index )
		bb_tickers['vendor_symbol'] = output['ticker'] + ' US Equity'
		bb_tickers['vendor_code_id'] = 4
		bb_tickers['trading_place_id'] = 0
		bb_tickers['effective_date'] = datetime.datetime.today().date()
		bb_tickers['end_date'] = None
		bb_tickers['vendor_symbol_source_id'] = 6
		bb_tickers = bb_tickers.reset_index()
		bb_tickers.columns = ['instrument_id','vendor_symbol','vendor_code_id','trading_place_id','effective_date','end_date','vendor_symbol_source_id']
		bb_tickers.to_sql( 'vendor_mappings', secdb.dbh, if_exists='append', index=False )

		logger.info( f'Inserted {len(bb_tickers)} Bloomberg tickers to vendor mappings table' )

		#Add related YAHOO codes for new symbols
		yahoo_code = pd.DataFrame( index=output.index )
		yahoo_code['vendor_symbol'] = output['ticker'].str.replace('/','-')
		yahoo_code['vendor_code_id'] = 10
		yahoo_code['trading_place_id'] = 0
		yahoo_code['effective_date'] = datetime.datetime.today().date()
		yahoo_code['end_date'] = None
		yahoo_code['vendor_symbol_source_id'] = 7
		yahoo_code = yahoo_code.reset_index()
		yahoo_code.columns = ['instrument_id','vendor_symbol','vendor_code_id','trading_place_id','effective_date','end_date','vendor_symbol_source_id']
		yahoo_code.to_sql( 'vendor_mappings', secdb.dbh, if_exists='append', index=False )

		logger.info( f'Inserted {len(yahoo_code)} YAHOO codes to vendor mappings table' )

	if len(symbol_dels) > 0:
		#Process deleted symbols
		symbol_dels['status_code'] = 'D'
		symbol_dels['version'] += 1
		symbol_dels['timestamp'] = datetime.datetime.now()

		for symbol in symbol_dels['ticker'].values:
			logger.info( f'Deactivating symbol {symbol}' )

		symbol_dels.to_sql( 'instruments', secdb.dbh, if_exists='append', index=False )
		logger.info( f'Deactivated {len(symbol_dels)} symbols in the intruments table' )

		#Expire and vendor mappings for deleted symbols
		instrument_ids = ','.join( [str(id) for id in symbol_dels['instrument_id'].values] )
		update_sql = f'update vendor_mappings set end_date = datetime() where instrument_id in ({instrument_ids}) and end_date is null'
		cursor = secdb.dbh.cursor()
		cursor.execute(update_sql)

	if len(ticker_changes) > 0:
		change_inserts = instruments[instruments['figi'].isin(ticker_changes.index)].copy()
		change_inserts['version'] += 1 
		change_inserts['ticker'] = change_inserts['figi'].map( ticker_changes['symbol'] )
		change_inserts['fsymbol'] = change_inserts['ticker'] + '-US'
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

		bb_tickers['vendor_symbol'] = bb_tickers['instrument_id'].map( change_inserts.set_index('instrument_id')['ticker'] ) + ' US Equity'
		bb_tickers['effective_date'] = datetime.datetime.today().date()
		bb_tickers.to_sql( 'vendor_mappings', secdb.dbh, if_exists='append', index=False )
		logger.info( f'Renamed {len(bb_tickers)} Bloomberg tickers in vendor mappings table' )

		#Update Related YAHOO symbols
		instrument_ids = ','.join( [str(id) for id in change_inserts['instrument_id'].values] )
		sql = f'select * from vendor_mappings where vendor_code_id = 10 and instrument_id in ({instrument_ids}) and end_date is null'
		yahoo_code = pd.read_sql( sql, secdb.dbh )

		update_sql = f'update vendor_mappings set end_date = datetime() where vendor_code_id = 10 and instrument_id in ({instrument_ids}) and end_date is null'
		cursor = secdb.dbh.cursor()
		cursor.execute(update_sql)

		yahoo_code['vendor_symbol'] = yahoo_code['instrument_id'].map( change_inserts.set_index('instrument_id')['ticker'] ).str.replace('/','-')
		yahoo_code['effective_date'] = datetime.datetime.today().date()
		yahoo_code.to_sql( 'vendor_mappings', secdb.dbh, if_exists='append', index=False )
		logger.info( f'Renamed {len(yahoo_code)} YAHOO codes in vendor mappings table' )

	secdb.dbh.commit()

if __name__ == "__main__":
	main()
