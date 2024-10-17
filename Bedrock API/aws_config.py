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