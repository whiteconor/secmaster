import datetime
import pandas as pd
import logging
from lib.openfigi import OpenFIGI

openfigi_api_key = '53e5a556-fb8a-4a8e-a173-e11aea0fd046'

def otc_figi_query( row ):
	return { 'idType' : 'TICKER', 'idValue': row['Symbol'], 'exchCode' : 'US', 'marketSecDes' : 'Equity', 'includeUnlistedEquities' : True }

def get_exchange_code( row ):
	if row['Listing Exchange'] == 'Q':
		if row['Market Category'] == 'Q':
			return 'UW'
		elif row['Market Category'] == 'G':
			return 'UQ'
		elif row['Market Category'] == 'S':
			return 'UR'
		else:
			return 'UQ'
	elif row['Listing Exchange'] == 'Z':
		return 'UF'
	else:
		return 'U' + row['Listing Exchange']

def main():

	logger = logging.getLogger( 'us_listed_equities_download' )
	logger.setLevel( logging.DEBUG )
	log_formatter = logging.Formatter( '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
	log_handler = logging.StreamHandler()
	log_handler.setFormatter( log_formatter )
	logger.addHandler( log_handler )
 
	openfigi = OpenFIGI( openfigi_api_key, logger )

	#Pull full list of Nasdaq traded equities from Nasdaq FTP site (includes most listed equities products in US)
	nasdaq_eq = pd.read_csv( 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt', sep='|', skipfooter=1, engine='python' )
	logger.info( f'Loaded {len(nasdaq_eq)} symbols from Nasdaq FTP site' )

	#Set primary exchange code based on single char exchange code in file
	nasdaq_eq['primary_exch_code'] = nasdaq_eq.apply( lambda row: get_exchange_code( row ), axis = 1 )

	#Replace '.' with '/' in symbols to match Bloomberg/OpenFIGI ticker symbology
	nasdaq_eq['Symbol'] = nasdaq_eq['Symbol'].str.replace( '.', '/' )

	#Filter out exchange test symbols
	nasdaq_eq = nasdaq_eq[nasdaq_eq['Test Issue']=='N'][['Symbol','primary_exch_code']]
	nasdaq_eq = nasdaq_eq[nasdaq_eq.Symbol.notna()]

	#Map symbols to OpenFIGI
	eq_figi, exceptions = openfigi.map_symbols( nasdaq_eq, otc_figi_query )

	#Update primary exchange from original data frame
	eq_figi['primary_exch_code'] = eq_figi['symbol'].map( nasdaq_eq.set_index('Symbol')['primary_exch_code'] )

	if len( exceptions ) > 0:
		logger.warning( 'OpenFIGI Exceptions' )

		for error, count in exceptions['error'].value_counts().items():
			logger.warning( f'{error} : {count}' )

	date_str = datetime.date.today().strftime( '%Y%m%d' )
	output_file = f'/data/us_equities_figi_mapped_{date_str}.csv'
	logger.info( f'Saving data to file {output_file}' )
	eq_figi.to_csv( output_file, index=False )
	exceptions.to_csv( f'/data/us_equities_exceptions_{date_str}.csv',  index=False )

if __name__ == "__main__":
	main()
