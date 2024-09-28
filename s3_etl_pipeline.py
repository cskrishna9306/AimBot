import gzip
import json
from io import BytesIO
import boto3
import botocore
from botocore.errorfactory import ClientError

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

                # Download the gzip file from the source S3 bucket
                gzip_obj = s3_client.get_object(Bucket=SOURCE_S3_BUCKET, Key=key)
                gzip_content = gzip_obj['Body'].read()

                # Unzip the content
                with gzip.GzipFile(fileobj=BytesIO(gzip_content), mode='rb') as gzip_file:
                    # Upload the unzipped content to the destination S3 bucket
                    s3_client.put_object(Bucket=DESTINATION_S3_BUCKET, Key=key[:-3], Body=gzip_file.read())
                    print(f"Uploaded unzipped file to {DESTINATION_S3_BUCKET}/{key[:-3]}")
    pass

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
            # Download the gzip file from the source S3 bucket
            gzip_obj = s3_client.get_object(Bucket=SOURCE_S3_BUCKET, Key=f'{tour}/esports-data/{file}.json.gz')
            gzip_content = gzip_obj['Body'].read()

            # Unzip the content
            with gzip.GzipFile(fileobj=BytesIO(gzip_content), mode='rb') as gzip_file:
                JSON = json.load(gzip_file)

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
                    pass
                case 'mapping_data':
                    for game in JSON:
                        GAMES[game['platformGameId']] = {
                            'tournament': TOURNAMENTS[game['tournamentId']]['name'],
                            'region': LEAGUES[TOURNAMENTS[game['tournamentId']]['league_id']]['region'],
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
            # Upload the PLAYERS dictionary to our destination S3 bucket
            s3_client.put_object(Bucket=DESTINATION_S3_BUCKET, Key=f'{tour}/player_metadata.json', Body=json.dumps(PLAYERS))
            print(f"Uploaded {tour}' players metadata file to s3://{DESTINATION_S3_BUCKET}/{tour}/player_metadata.json")
        
        pass

    def game_data_etl():
        pass
    
    # loading leagues, tournaments, teams, and players data from this tour into the cache
    esports_data_etl(load = True)
    # creating player statistics from each game
    game_data_etl()
    pass

if __name__ == "__main__":
    # extract, unzip, and load the fandom data
    fandom_data_etl()

    # extract, unzip, and transform each of the tour data
    tour_data_etl(tour for tour in TOURS)