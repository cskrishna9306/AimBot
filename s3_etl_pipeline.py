import gzip
import json
from io import BytesIO
import boto3
import botocore
from botocore.errorfactory import ClientError
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from datetime import datetime

# Initializing the S3 client
s3_client = boto3.client('s3')
# Create a paginator for listing objects
paginator = s3_client.get_paginator('list_objects_v2')

# the source S3 bucket with the zipped data to extract from
SOURCE_S3_BUCKET = 'vcthackathon-data'
# the destination bucket to store the unzipped and transformed data
DESTINATION_S3_BUCKET = 'esports-digital-assistant-data'

# the 3 tours/tiers within VCT and their respective active years
TOURS = {
    'game-changers': [2022, 2023, 2024],
    'vct-challengers': [2023, 2024],
    'vct-international': [2022, 2023, 2024]
}

# the order to traverse the files in esports-data
ESPORTS_DATA = ['leagues', 'tournaments', 'teams', 'players', 'mapping_data']

# Function definition to return the unzipped data
def extract_zipped_data(bucket, key):
    try:
        # Download the gzip file from the source S3 bucket
        gzip_obj = s3_client.get_object(Bucket=bucket, Key=key)
        gzip_content = gzip_obj['Body'].read()
    
        # Unzip the gzipped content
        with gzip.GzipFile(fileobj=BytesIO(gzip_content)) as gzip_file:
            return gzip_file.read()
    
    except NoCredentialsError:
        print("Error: Credentials are not available")
    except PartialCredentialsError:
        print("Error: Incomplete credentials provided")
    except Exception as e:
        print(f"Error: {e}")
    return

# Function definition to extract zipped fandom data from the source S3 bucket, 
# and load the unzipped data into our destination S3 bucket
def fandom_data_etl():
    # Traverse the fandom file path in the source S3 bucket
    # Use the paginator to iterate over all objects within the fandom prefix
    for page in paginator.paginate(Bucket=SOURCE_S3_BUCKET, Prefix='fandom/'):
        # Check if the page contains objects
        if 'Contents' in page:
            for object in page['Contents']:
                key = object['Key']
                
                # extract, unzip, and load the unzipped data into the destination S3 bucket
                s3_client.put_object(Bucket=DESTINATION_S3_BUCKET, Key=key[:-3], Body=extract_zipped_data(bucket=SOURCE_S3_BUCKET, key=key))
                print(f"Uploaded unzipped file to {DESTINATION_S3_BUCKET}/{key[:-3]}")
    return

# Function definition to extract zipped tour data from the source S3 bucket,
# transform the data, and load player statistics into the destination S3 bucket
def tour_data_etl(tour):

    # Hashmap of all the leagues in this tour
    LEAGUES = {}
    # Hashmap of all the tournaments in this tour
    TOURNAMENTS = {}
    # Hashmap of all the teams participating in this tour
    TEAMS = {}
    # Hashmap of all the players participating in this tour
    PLAYERS = {}
    # Hashmap of all the games played in this tour
    GAMES = {}

    # Function definition to load esports data from this tour into one of the above hashmaps
    def esports_data_etl(load = False):
        # Traverse the esports-data by leagues, tournaments, teams, players, and finally games
        for file in ESPORTS_DATA:
            # EXRTACTION PHASE
            JSON = json.loads(extract_zipped_data(SOURCE_S3_BUCKET, f'{tour}/esports-data/{file}.json.gz'))
            
            # TRANSFORMATION PHASE
            match file:
                case 'leagues':
                    for league in JSON:
                        LEAGUES[league['league_id']] = {
                            'name': league['name'],
                            'region': league['region']
                        }
                case 'tournaments':
                    for tournament in JSON:
                        TOURNAMENTS[tournament['id']] = {
                            'name': tournament['name'],
                            'league_name': LEAGUES[tournament['league_id']]['name'],
                            # NOTE: we may or may not need this
                            'region': LEAGUES[tournament['league_id']]['region']
                        }
                case 'teams':
                    for team in JSON:
                        TEAMS[team['id']] = {
                            'name': team['name'],
                            'acronym': team['acronym'],
                            'home_league_name': LEAGUES[team['home_league_id']]['name'],
                            'region': LEAGUES[team['home_league_id']]['region']
                        }
                case 'players':
                    # TODO: figure out logic to map the players to the right team for that year
                    # owing to increasing complexity of keeping track of the changing teams for each player, 
                    # we decided to only retain the most recent information of the player
                    for player in JSON:
                        # get the 'created_at' for this player entry
                        date = datetime.strptime(player['updated_at'], "%Y-%m-%dT%H:%M:%SZ")
                        if (player['id'] in PLAYERS and date > PLAYERS[player['id']]['date']) or player['id'] not in PLAYERS:
                                PLAYERS[player['id']] = {
                                    'handle': player['handle'],
                                    'date': date,
                                    'status': player['status'],
                                    'first_name': player['first_name'],
                                    'last_name': player['last_name'],
                                    'home_team_name': TEAMS[player['home_team_id']]['name'] if player['home_team_id'] in TEAMS else None,
                                    'home_team_acronym': TEAMS[player['home_team_id']]['acronym'] if player['home_team_id'] in TEAMS else None,
                                    'home_league_name': TEAMS[player['home_team_id']]['home_league_name'] if player['home_team_id'] in TEAMS else None,
                                    'region': TEAMS[player['home_team_id']]['region'] if player['home_team_id'] in TEAMS else None,
                                    'game_statistics': []
                                }
                case 'mapping_data':
                    for game in JSON:
                        GAMES[game['platformGameId']] = {
                            'tournament': TOURNAMENTS[game['tournamentId']]['name'],
                            'region': TOURNAMENTS[game['tournamentId']]['region'],
                            # NOTE: We may or may not need the teams field since we can map each player to their respective teams with the PLAYERS dict
                            'teams': {
                                int(localTeamID): TEAMS[teamID]['name'] for localTeamID, teamID in game['teamMapping'].items() if teamID in TEAMS
                            },
                            'players': {
                                int(localPlayerID): PLAYERS[playerID]['handle'] for localPlayerID, playerID in game['participantMapping'].items() if playerID in PLAYERS
                            }
                        }
            
        # LOADING PHASE: Optionally LOAD players metadata into the destination S3 bucket
        if load:
            PLAYERS_W_DATES = {}
            for pID, pInfo in PLAYERS.items():
                PLAYERS_W_DATES[pID] = {}
                # iterate through key-value pairs in pInfo
                for key, value in pInfo.items():
                    PLAYERS_W_DATES[pID][key] = value.isoformat() if isinstance(value, datetime) else value
            
            # Upload the PLAYERS dictionary to our destination S3 bucket
            s3_client.put_object(Bucket=DESTINATION_S3_BUCKET, Key=f'{tour}/player_metadata.json', Body=json.dumps(PLAYERS_W_DATES))
            print(f"Uploaded {tour}' players' metadata information to s3://{DESTINATION_S3_BUCKET}/{tour}/player_metadata.json")
        
        return

    def game_data_etl():
        # parse though each game within GAMES
        for game in GAMES:
            # check for a hit for this game within {tour}/games/[2022, 2023, 2024]
            for year in TOURS[tour]:
                try:
                    s3_client.head_object(Bucket=SOURCE_S3_BUCKET, key='{tour}/games/{year}/{game}.json.gz')
                    # upon a hit, we extract, and perform transformation on the unzipped data
                    
                    break
                except botocore.exceptions.ClientError as e:
                    pass
            
        return
    
    # loading leagues, tournaments, teams, and players data from this tour into the cache
    esports_data_etl()
    # creating player statistics from each game
    game_data_etl()
    return

if __name__ == "__main__":
    # extract, unzip, and load the fandom data
    fandom_data_etl()

    # extract, unzip, and transform each of the tour data
    for tour in TOURS:
        tour_data_etl(tour)