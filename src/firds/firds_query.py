import requests
import datetime
from typing import List

url = 'https://api.data.fca.org.uk/search?index=fca-firds-downloadfiles'

class FIRDSException( Exception ):
    pass
 
def search_firds_files( pub_date:datetime.datetime, file_type:str ) -> List[str]:
    query = {
		'from': 0,
		'size': '100',
		'sort': 'publication_date',
		'keyword': None,
		'sortorder': 'asc',
		'criteriaObj': {
			'criteria': [
				{
					'name': 'file_type', 
					'value': 'FULINS'
				}
			],
			'dateCriteria': [
				{
					'name': 'publication_date',
					'value': {
						'from': pub_date.strftime( '%d/%m/%Y' ),
						'to': pub_date.strftime( '%d/%m/%Y' )
					}
				}
			]
		}
	}

    res = requests.post( url, json=query )
    
    if res.status_code == 200:
        file_list = res.json()['hits']['hits']
        print(f'Found {len(file_list)} FIRDS files')
        return file_list
    else:
        msg = f'FIRDS file search failed: {res.status_code} : {res.text}'
        raise FIRDSException( msg )