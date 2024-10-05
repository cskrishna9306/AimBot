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

# agent code mappings
with open('../Valorant Metadata/agent_code_mapping.json', 'r') as file:
    AGENT_CODE_MAPPINGS = json.load(file)

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
            JSON_FILE = json.loads(extract_zipped_data(SOURCE_S3_BUCKET, f'{tour}/esports-data/{file}.json.gz'))
            
            # TRANSFORMATION PHASE
            match file:
                case 'leagues':
            # if file == 'leagues':
                    for league in JSON_FILE:
                        LEAGUES[league['league_id']] = {
                            'name': league['name'],
                            'region': league['region']
                        }
                case 'tournaments':
            # elif file == 'tournaments':
                    for tournament in JSON_FILE:
                        TOURNAMENTS[tournament['id']] = {
                            'name': tournament['name'],
                            'league_name': LEAGUES[tournament['league_id']]['name'],
                            # NOTE: we may or may not need this
                            'region': LEAGUES[tournament['league_id']]['region']
                        }
                case 'teams':
            # elif file == 'teams':
                    for team in JSON_FILE:
                        TEAMS[team['id']] = {
                            'name': team['name'],
                            'acronym': team['acronym'],
                            'home_league_name': LEAGUES[team['home_league_id']]['name'],
                            'region': LEAGUES[team['home_league_id']]['region']
                        }
                case 'players':
            # elif file == 'players':
                    # TODO: figure out logic to map the players to the right team for that year
                    # owing to increasing complexity of keeping track of the changing teams for each player, 
                    # we decided to only retain the most recent information of the player
                    for player in JSON_FILE:
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
            # elif file == 'mapping_data':
                    for game in JSON_FILE:
                        GAMES[game['platformGameId']] = {
                            'tournament': TOURNAMENTS[game['tournamentId']]['name'],
                            'region': TOURNAMENTS[game['tournamentId']]['region'],
                            # NOTE: We may or may not need the teams field since we can map each player to their respective teams with the PLAYERS dict
                            'teams': {
                                int(localTeamID): teamID for localTeamID, teamID in game['teamMapping'].items()
                            },
                            'players': {
                                int(localPlayerID): playerID for localPlayerID, playerID in game['participantMapping'].items()
                            }
                        }
            
        for pID, pInfo in PLAYERS.items():
            # iterate through key-value pairs in pInfo
            for key, value in pInfo.items():
                PLAYERS[pID][key] = value.isoformat() if isinstance(value, datetime) else value
        
        # LOADING PHASE: Optionally LOAD players metadata into the destination S3 bucket
        if load:
            # Upload the PLAYERS dictionary to our destination S3 bucket
            s3_client.put_object(Bucket=DESTINATION_S3_BUCKET, Key=f'{tour}/player_metadata.json', Body=json.dumps(PLAYERS))
            print(f"Uploaded {tour}' players' metadata information to s3://{DESTINATION_S3_BUCKET}/{tour}/player_metadata.json")
        
        return

    # Function definition to load and transform player specific data
    def game_data_etl():
        i = 0
        # parse though each game within GAMES
        for game, game_metadata in GAMES.items():
            # check for a hit for this game within {tour}/games/[2022, 2023, 2024]
            for year in TOURS[tour]:
                try:
                    # EXTRACTION PHASE
                    # check for this file within this year's directory
                    s3_client.head_object(Bucket=SOURCE_S3_BUCKET, Key=f'{tour}/games/{year}/{game}.json.gz')
                    
                    # upon a hit, we extract, and perform transformation on the unzipped data
                    gameJSON = json.loads(extract_zipped_data(SOURCE_S3_BUCKET, f'{tour}/games/{year}/{game}.json.gz'))
                    
                    # TRANSFORMATION PHASE
                    # Assuming players 6 - 10 are always assigned the lower team number and start as attacker (vice versa for players 1 - 5)
                    # NOTE: DISCARD THE USE OF GAME METADATA HERE
                    
                    # THE PROBLEM IS THAT WHEN ADDING THE TEAMS
                    team_player_mappings = {
                        min(game_metadata['teams'].keys()): {6, 7, 8, 9, 10},
                        max(game_metadata['teams'].keys()): {1, 2, 3, 4, 5}
                    }

                    attacking_team = min(game_metadata['teams'].keys())

                    # Hashmap containing the stats per player for this game
                    game_summary = {
                        localPlayerID: {
                            'kills': {
                                'attack': 0,
                                'defense': 0
                            },
                            'deaths': {
                                'attack': 0,
                                'defense': 0
                            },
                            'assists': {
                                'attack': 0,
                                'defense': 0
                            },
                            'rounds_won': {
                                'attack': 0,
                                'defense': 0
                            },
                            # TODO: maybe add revives as a performance metric (something like avg. revive rate per round)
                            # TODO: add stats for total damage dealt this game
                            # TODO: add stats for down to death ratio
                            # TODO: add stats for first blood percentages in each round
                            'combat_score': 0,
                            'map': None,
                            'agent': None,
                            'role': None,
                            'tournament': game_metadata['tournament'],
                            'region': game_metadata['region'],
                        } for localPlayerID in range(1, 11)
                    }

                    config_handled = False
                    # run game analytics for this game adhering with the local player and team IDs
                    for event in gameJSON:
                        if 'configuration' in event and not config_handled:
                            # Ensure that the 'configuration' event is only handled once
                            config_handled = True
                            
                            for localPlayerID, value in game_summary.items():
                                # assign map information for this game
                                value['map'] = event['configuration']['selectedMap']['fallback']['displayName']
                                
                            # assign agent information per player to their respective summaries
                            for player in event['configuration']['players']:
                                if player['selectedAgent']['fallback']['guid'] in AGENT_CODE_MAPPINGS:
                                    # Associating player with agent name
                                    game_summary[player['playerId']['value']]['agent'] = AGENT_CODE_MAPPINGS[player['selectedAgent']['fallback']['guid']]['name']
                                    # Associating player with agent role
                                    game_summary[player['playerId']['value']]['role'] = AGENT_CODE_MAPPINGS[player['selectedAgent']['fallback']['guid']]['role']
                        elif 'roundStarted' in event:
                            # if attacking team is min team number we are talking about 6 - 10
                            # else attacking team is max team number we are talking about players 1 - 5
                            attacking_team = event['roundStarted']['spikeMode']['attackingTeam']['value']
                        elif 'playerDied' in event:
                            deceasedId = event['playerDied']['deceasedId']['value']
                            killerId = event['playerDied']['killerId']['value']
                            game_summary[deceasedId]['deaths']['attack' if deceasedId in team_player_mappings[attacking_team] else 'defense'] += 1
                            game_summary[killerId]['kills']['attack' if deceasedId in team_player_mappings[attacking_team] else 'defense'] += 1
                        elif 'playerRevived' in event:
                            pass
                        elif 'roundDecided' in event:
                            for player in team_player_mappings[event['roundDecided']['result']['winningTeam']['value']]:
                                game_summary[player]['rounds_won']['attack' if player in team_player_mappings[attacking_team] else 'defense'] += 1
                        elif 'snapshot' in event:
                            for player in event['snapshot']['players']:
                                game_summary[player['playerId']['value']]['combat_score'] = player['scores']['combatScore']['totalScore']
                    
                    # Joining each player's statistics from this game to their respective entries in PLAYERS
                    for localPlayerID, playerID in game_metadata['players'].items():
                        if playerID in PLAYERS:
                            # Calculate player KDA for this game
                            # attack kills + attack assists / attack deaths
                            game_summary[localPlayerID]['attack_kda'] = round((game_summary[localPlayerID]['kills']['attack'] + game_summary[localPlayerID]['assists']['attack']) / max(1, game_summary[localPlayerID]['deaths']['attack']), 2)
                            # defense kills + defense assists / defense deaths
                            game_summary[localPlayerID]['defense_kda'] = round((game_summary[localPlayerID]['kills']['defense'] + game_summary[localPlayerID]['assists']['defense']) / max(1, game_summary[localPlayerID]['deaths']['defense']), 2)
                            
                            # delete the kills/deaths/assists stats
                            del game_summary[localPlayerID]['kills']
                            del game_summary[localPlayerID]['deaths']
                            del game_summary[localPlayerID]['assists']

                            PLAYERS[playerID]['game_statistics'].append(game_summary[localPlayerID])
                    
                    print(f'Succesfully retreived player stats from {tour}/games/{year}/{game}.json.gz')
                    i += 1
                    # need to only find the first hit
                    break
                except botocore.exceptions.ClientError as e:
                    if year == 2024:
                        print(f'Error: File for {game} not found')
            
            if i == 100:
                break

        # LOADING PHASE
        PLAYERS_LIST = []
        for _, player in PLAYERS.items():
            PLAYERS_LIST.append(player)
        
        # Upload the PLAYERS dictionary to our destination S3 bucket
        s3_client.put_object(Bucket=DESTINATION_S3_BUCKET, Key=f'{tour}/player_statistics.json', Body=json.dumps(PLAYERS_LIST))
        print(f"Uploaded {tour}' players' statistics information to s3://{DESTINATION_S3_BUCKET}/{tour}/player_statistics.json")

        return
    
    # loading leagues, tournaments, teams, and players data from this tour into the cache
    esports_data_etl()
    # creating player statistics from each game
    game_data_etl()
    return

if __name__ == "__main__":
    # extract, unzip, and load the fandom data
    fandom_data_etl()

    # extract, unzip, and transform each tour data
    for tour in TOURS:
        tour_data_etl(tour)