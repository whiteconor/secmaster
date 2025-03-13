import requests
import pandas as pd
import numpy as np
import logging
import time
import datetime

exchange_codes = ['GA','AU','AV','BB','SW','CN','CH','DC','FH','FP','GR','HK','ID','IM','IN','IT','JP','JR','SS','KS','LN','LX','MM','NA','NO','NQ','NZ','PL','PW','SJ','SM','SP','TT','VX']
security_types = ['Common Stock','ETP','ADR','GDR']

api_key = '53e5a556-fb8a-4a8e-a173-e11aea0fd046'
openfigi_url = 'https://api.openfigi.com/v3/filter'
openfigi_headers = {'Content-Type': 'text/json', 'X-OPENFIGI-APIKEY' : api_key }


def main():
    
    logger = logging.getLogger( 'non_us_equity_search' )
    logger.setLevel( logging.DEBUG )
    log_formatter = logging.Formatter( '%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
    log_handler = logging.StreamHandler()
    log_handler.setFormatter( log_formatter )
    logger.addHandler( log_handler )
    
    run_date = datetime.date.today().strftime( '%Y%m%d' )
    
    np.random.shuffle( exchange_codes )
    
    for exchange_code in exchange_codes:
        result_set = []
        
        logger.info(f'Processing exchange code {exchange_code}')
    
        for security_type in security_types:
            logger.info(f'Processing security type {security_type}')
            
            query = {
                'marketSecDes' : 'Equity',
                'securityType' : security_type,
                'exchCode' : exchange_code
            }

            res = requests.post( url=openfigi_url, headers=openfigi_headers, json=query )

            if res.status_code == 200:
                results = res.json()
                count = results['total']
                logger.info(f'{count} securities found')
                if 'next' in results:
                    start = results['next']
                else:
                    start = None

                result_set.extend( results['data'] )

                while start is not None:
                     #Sleep for a random period between 3 and 10 seconds. This is to get around rate limiting on the OpenFIGI API
                    time.sleep( np.random.uniform(3,10))
                    
                    query['start'] = start
                    res = requests.post( url=openfigi_url, headers=openfigi_headers, json=query )
                    if res.status_code == 200:
                        results = res.json()

                        if 'next' in results:
                            start = results['next']
                        else:
                            start = None
            
                        result_set.extend( results['data'] )
                    else:
                        logger.error(f'{res.text}')
                        start = None
            else:
                logger.error(f'{res.text}')


        df = pd.DataFrame( result_set )
        df.to_csv( f'/data/non_us_equities_{exchange_code}_{run_date}.csv', index=False )
        #Sleep for a random period between 1 and 5 minutes. This is to get around rate limiting on the OpenFIGI API
        wait_time = np.random.uniform(60,300)
        logger.info(f'Waiting for {wait_time:.2f} seconds')
        time.sleep( wait_time )

if __name__ == '__main__':
    main()