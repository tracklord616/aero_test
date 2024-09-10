import requests
from psycopg2 import connect


def get_data(api_url: str) -> dict:
    try:
        response = requests.get(api_url)
        if response.status_code != 200:
            raise ValueError(response.text)
        data = response.json()
        return data['stats']
    except Exception:
        raise

def upload_data(data: tuple,
                cursor: connect.cursor,
                table_name: str) -> None:
    try:
        cursor.execute(f'insert into stg."{table_name}" values {data}')
    except Exception:
        raise

    
def process_data(data: dict) -> dict:
    try:
        gametype_values = [data.get('type').get('gameType').get('id'), 
                           data.get('type').get('gameType').get('description'), 
                           data.get('type').get('gameType').get('postseason')]

        for team in data['splits']:
            team_values = [team.get('team').get('id'),
                           team.get('team').get('name')
                           ]
        stat_values = list(team.get('stat').values())
        season_data = tuple(team_values + gametype_values + stat_values)
        ranking_data = tuple(team_values + stat_values)

        return {'season_data': season_data,
                'ranking_data': ranking_data}
    except Exception:
        raise
        
        
def etl_pipeline(url: str,
                 credentials: dict):
    try:

        loaded_data = get_data(api_url=url)
        processed_data = process_data(loaded_data)
        conn = connect(host=credentials['host'], 
                       port=credentials['port'], 
                       dbname=credentials['dbname'], 
                       user=credentials['user'], 
                       password=credentials['password'])
        with conn.cursor() as cur:
            upload_data(data=processed_data['season_data'],
                        cursor=cur,
                        table_name='season_data')
            upload_data(data=processed_data['ranking_data'],
                        cursor=cur,
                        table_name='ranking_data')

    except Exception:
        raise
test_url = ''
test_credentials = {}

if __name__ == '__main__':
    etl_pipeline(url=test_url,
                 credentials=test_credentials)