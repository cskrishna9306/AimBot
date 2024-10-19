import boto3
from opensearchpy import AWSV4SignerAuth

# Primary region of choice
REGION = 'us-west-2'

# Bucket name w/ the dataset
S3_BUCKET = 'esports-digital-assistant-data'

# AOSS vector store name
AOSS_COLLECTION = {
    'name': 'bedrock-rag-collection',
    'id': None,
    'arn': None
}
# AOSS index name
INDEX_NAME = 'bedrock-rag-index'
# AOSS security policy names
AOSS_POLICY_NAMES = [
    {
        'name': 'bedrock-rag-encryption-policy',
        'type': 'encryption'
    },
    {
        'name': 'bedrock-rag-network-policy',
        'type': 'network'
    },
    {
        'name': 'bedrock-rag-access-policy',
        'type': 'data'
    }
]

# Bedrock Knowledge Base
BEDROCK_KB = {
    'name': 'esports-digital-assistant-kb',
    'description': 'Knowledge base containing player statistics from VCT game-changers, challengers, and international.',
    'id': None,
    'arn': None
}
# Bedrock KB Data source
BEDROCK_KB_DATA_SOURCE = {
    'name': 'esports-digital-assistant-kb-ds',
    'description': 'The data source for the esports-digital-assistant-kb',
    'id': None,
    'arn': None
}
# Bedrock KB Exection role
BEDROCK_KB_EXECUTION_ROLE = {
    'name': 'Bedrock-Execution-Role-KB',
    'arn': None
}
# Bedrock KB policy names
BEDROCK_KB_POLICY_NAMES = ['Bedrock-FM-Policy-KB', 'Bedrock-S3-Policy-KB', 'Bedrock-OSS-Policy-KB']

# Bedrock Agent
BEDROCK_AGENT = {
    'name': 'esports-digital-assistant',
    'description': 'Valorant Esports Digital Assistant that is responsible for creating a Valorant team of 5 players with the best overall performance.',
    'instruction': 'You are an esports digital assistant tasked with gathering player statistics and creating a team of 5 players with best overall performance.',
    'id': None,
    'arn': None
}
# Bedrock Agent Execution Role
BEDROCK_AGENT_EXECUTION_ROLE = {
    'name': 'Bedrock-Execution-Role-Agent',
    'arn': None
}
# Bedrock Agent Policy names
BEDROCK_AGENT_POLICY_NAMES = ['Bedrock-FM-Policy-Agent', 'Bedrock-KB-Policy-Agent']

# The FM used by the Bedrock agent
FOUNDATION_MODEL = 'anthropic.claude-3-sonnet-20240229-v1:0'

# Initializing the IAM client (global service)
iam_client = boto3.client('iam')
# Initialzing the STS client (global service I think)
sts_client = boto3.client('sts')
# Initializing the S3 client (global service)
s3_client = boto3.client('s3')
# Initializing the Amazon OpenSearch Serverless client
aoss_client = boto3.client('opensearchserverless', region_name=REGION)
# Initialixing the Bedrock Agent client
bedrock_agent_client = boto3.client('bedrock-agent', region_name=REGION)

awsauth = auth = AWSV4SignerAuth(boto3.Session().get_credentials(), REGION, 'aoss')

# this account name
ACCOUNT_ID = sts_client.get_caller_identity()['Account']
# this account's ARN
IDENTITY = sts_client.get_caller_identity()['Arn']

# vct hackathon tags
TAGS_UPPER_CASE = [
    {
        'Key': 'vct-hackathon',
        'Value': '2024'
    }
]

TAGS_LOWER_CASE = [
    {
        'key': 'vct-hackathon',
        'value': '2024'
    }
]

TAGS_DICT = {
    'vct-hackathon': '2024'
}