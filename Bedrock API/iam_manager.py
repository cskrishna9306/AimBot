import json
from botocore.exceptions import ClientError
from aws_config import *

# NOTE: This could be a possible way to orchestrate policy creation and attack it to its respective roles
# we can have one policy document governing the fm policies, and s3 policy
# to attach oss policy we need to create a collection first

def create_bedrock_kb_execution_role():
    IAM_POLICIES = [
        {
            'name': 'Bedrock-FM-Policy-KB',
            'arn': None,
            'description': 'Policy for accessing foundation model',
            'document': {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "bedrock:ListFoundationModels",
                            "bedrock:ListCustomModels"
                        ],
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "bedrock:InvokeModel",
                        ],
                        "Resource": [
                            f"arn:aws:bedrock:{REGION}::foundation-model/amazon.titan-embed-text-v1",
                            f"arn:aws:bedrock:{REGION}::foundation-model/amazon.titan-embed-text-v2:0"
                        ]
                    }
                ]
            }
        },
        {
            'name': 'Bedrock-S3-Policy-KB',
            'arn': None,
            'description': 'Policy for reading documents from S3',
            'document': {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:ListBucket"
                        ],
                        "Resource": [
                            f"arn:aws:s3:::{S3_BUCKET}",
                            f"arn:aws:s3:::{S3_BUCKET}/*"
                        ],
                        "Condition": {
                            "StringEquals": {
                                "aws:ResourceAccount": f"{ACCOUNT_ID}"
                            }
                        }
                    }
                ]
            }
        },
        {
            'name': 'Bedrock-OSS-Policy-KB',
            'arn': None,
            'description': 'Policy for accessing opensearch serverless',
            'document': {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "aoss:APIAccessAll"
                        ],
                        "Resource": [
                            f"arn:aws:aoss:{REGION}:{ACCOUNT_ID}:collection/{AOSS_COLLECTION['id']}"
                        ]
                    }
                ]
            }
        }
    ]
    
    def delete_bedrock_kb_execution_role():
        # First, detach all the attached policies to the IAM role
        for policy in IAM_POLICIES:
            try:
                iam_client.detach_role_policy(
                    RoleName=BEDROCK_KB_EXECUTION_ROLE['name'],
                    PolicyArn=f"arn:aws:iam::{ACCOUNT_ID}:policy/{policy['name']}"
                )
                print(f"Successfully detached the {policy['name']} IAM policy from the {BEDROCK_KB_EXECUTION_ROLE['name']} IAM role.")
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchEntityException':
                    print(f"Either the IAM role {BEDROCK_KB_EXECUTION_ROLE['name']} or the IAM policy {policy['name']} does not exist.")
                else:
                    # Handle any other error
                    print(f"An unexpected error occurred: {e}")
                    raise

        # Next, delete the Bedrock KB execution role
        try:
            iam_client.delete_role(RoleName=BEDROCK_KB_EXECUTION_ROLE['name'])
            print(f"Successfully deleted the {BEDROCK_KB_EXECUTION_ROLE['name']} IAM role.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchEntityException':
                print(f"The IAM role {BEDROCK_KB_EXECUTION_ROLE['name']} does not exist.")
            else:
                # Handle any other error
                print(f"An unexpected error occurred: {e}")
                raise
            
        # Finally, delete the IAM policies
        for policy in IAM_POLICIES:
            try:
                iam_client.delete_policy(PolicyArn=f"arn:aws:iam::{ACCOUNT_ID}:policy/{policy['name']}")
                print(f"Successfully deleted the {policy['name']} IAM policy.")
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchEntityException':
                    print(f"The IAM policy {policy['name']} does not exist.")
                else:
                    # Handle any other error
                    print(f"An unexpected error occurred: {e}")
                    raise
        
        return
    
    # first, create a clean slate by deleting the bedrock_kb_execution_role and all its attached IAM policies
    delete_bedrock_kb_execution_role()
    
    # now, create the FM, S3, and OSS IAM policies
    for policy in IAM_POLICIES:
        try:
            policy['arn'] = iam_client.create_policy(
                PolicyName=policy['name'],
                PolicyDocument=json.dumps(policy['document']),
                Description=policy['description'],
                Tags=TAGS_UPPER_CASE
            )['Policy']['Arn']
            print(f"Successfully created the {policy['name']} IAM policy.")
        except ClientError as e:
            # Check if the error code is 'EntityAlreadyExists'
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f"The {policy['name']} IAM policy already exists.")
            else:
              # Re-raise the error if it's not the expected one
                raise  
        
    # create the bedrock kb execution role
    try:
        BEDROCK_KB_EXECUTION_ROLE['arn'] = iam_client.create_role(
            RoleName=BEDROCK_KB_EXECUTION_ROLE['name'],
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "bedrock.amazonaws.com"
                            },
                            "Action": "sts:AssumeRole"
                        }
                    ]
                }
            ),
            Description='Amazon Bedrock Knowledge Base Execution Role for accessing OSS and S3',
            MaxSessionDuration=3600,
            Tags=TAGS_UPPER_CASE
        )['Role']['Arn']
        print(f"Successfully created the {BEDROCK_KB_EXECUTION_ROLE['name']} IAM role.")
    except ClientError as e:
        # Check if the error code is 'EntityAlreadyExists'
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print('The Bedrock KB IAM role already exists.')
            BEDROCK_KB_EXECUTION_ROLE['arn'] = iam_client.get_role(RoleName=BEDROCK_KB_EXECUTION_ROLE['name'])['Role']['Arn']
        else:
            # Re-raise the error if it's not the expected one
            raise 
    
    # attach the above created policies to Amazon Bedrock execution role
    for policy in IAM_POLICIES:
        try:
            iam_client.attach_role_policy(
                RoleName=BEDROCK_KB_EXECUTION_ROLE['name'],
                PolicyArn=policy['arn']
            )
            print(f"Succesfully attached the {policy['name']}.")
        except ClientError as e:
             # Check if the error code is 'EntityAlreadyExists'
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f"The {policy['name']} has already been attached to the bedrock execution role.")
            else:
                # Re-raise the error if it's not the expected one
                raise 
    
    return

def create_oss_policies():
    
    OSS_POLICIES = {
        'Encryption_Policy': {
            'name': 'bedrock-rag-encryption-policy',
            'policy': {
                'Rules': [
                    {
                        'Resource': ['collection/' + AOSS_COLLECTION['name']],
                        'ResourceType': 'collection'
                    }
                ],
                'AWSOwnedKey': True
            },
            'type': 'encryption'
        },
        'Network_Policy': {
            'name': 'bedrock-rag-network-policy',
            'policy': [
                {
                    'Rules': [
                        {
                            'Resource': ['collection/' + AOSS_COLLECTION['name']],
                            'ResourceType': 'collection'
                        }
                    ],
                    'AllowFromPublic': True
                }
            ],
            'type': 'network'
        },
        'Access_Policy': {
            'name': 'bedrock-rag-access-policy',
            'policy': [   
                {
                    'Rules': [
                        {
                            'Resource': ['collection/' + AOSS_COLLECTION['name']],
                            'Permission': ['aoss:*'],
                            'ResourceType': 'collection'
                        },
                        {
                            'Resource': ['index/' + AOSS_COLLECTION['name'] + '/*'],
                            'Permission': ['aoss:*'],
                            'ResourceType': 'index'
                        }
                    ],
                    'Principal': [IDENTITY, BEDROCK_KB_EXECUTION_ROLE['arn']],
                    'Description': 'Easy data policy'
                }
            ],
            'type': 'data'
        }
    }
    
    # Traverse through all the OSS Policies and accordingly create them
    for policy_name, policy in OSS_POLICIES.items():
        try:
            if policy_name != 'Access_Policy':
                aoss_client.create_security_policy(
                    name=policy['name'],
                    policy=json.dumps(policy['policy']),
                    type=policy['type']
                )
            else:
                aoss_client.create_access_policy(
                    name=policy['name'],
                    policy=json.dumps(policy['policy']),
                    type=policy['type']
                )
            print(f"Successfully created the AOSS {policy['name']}.")
        except ClientError as e:
            # Check for ConflictException
            if e.response['Error']['Code'] == 'ConflictException':
                print(f"{policy['name']} already exists.")
                
                if policy_name != 'Access_Policy':
                    # Delete this OSS policy
                    aoss_client.delete_security_policy(
                        name=policy['name'],
                        type=policy['type']  # 'encryption', 'network', or 'data' for security; 'access' for access policies
                    )
                    print(f"Deleted the AOSS {policy['name']}.")
                    
                    # Recreate this OSS policy
                    aoss_client.create_security_policy(
                        name=policy['name'],
                        policy=json.dumps(policy['policy']),
                        type=policy['type']
                    )
                else:
                    # Delete this OSS policy
                    aoss_client.delete_access_policy(
                        name=policy['name'],
                        type=policy['type']  # 'encryption', 'network', or 'data' for security; 'access' for access policies
                    )
                    print(f"Deleted the AOSS {policy['name']}.")
                    
                    # Recreate this OSS policy
                    aoss_client.create_access_policy(
                        name=policy['name'],
                        policy=json.dumps(policy['policy']),
                        type=policy['type']
                    )
                print(f"Successfully created the AOSS {policy['name']}.")
            else:
                # Handle other exceptions
                raise
    
    return