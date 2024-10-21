import boto3
import json
import time

def create_iam_policy(bkt_name):
    iam_client = boto3.client('iam')

    print('Creating policy')

    policy_document = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:*Object"
            ],
            "Resource": [
                "arn:aws:s3:::{bkt_name}/*"
            ]
        }
    ]
    }
    
    response = iam_client.create_policy(
                                PolicyName='S3FullAccessPolicy',
                                PolicyDocument=json.dumps(policy_document),  # Convert dict to JSON
                                Description='A policy to allow full access to S3 bucket.'
                            )
    print(f'Policy created successfully')
    return response['Policy']['Arn']

def create_iam_role(policy_arn):
    iam_client = boto3.client('iam')
    role_name = 'AWSGlueServiceRole-GhActivity'

    print('Creating role')

    # Trust policy that allows the specified service to assume this role
    trust_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
    }
    try:
        # Create the role
        create_role_response = iam_client.create_role(
                                            RoleName=role_name,
                                            AssumeRolePolicyDocument=json.dumps(trust_policy)
                                        )
        
        role_arn = create_role_response['Role']['Arn']
        print(f'Role {role_name} created successfully with ARN: {role_arn}')

    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f'Role already exists.')
        create_role_response = iam_client.get_role(RoleName=role_name)
    
    except Exception as e:
        print(f'Error: {e}')

    try:
        # Attach the specified policies to the role
        iam_client.attach_role_policy(
                                        RoleName=role_name,
                                        PolicyArn=policy_arn
                                    )
        print(f'Policy {policy_arn} attached to role "{role_name}".')

    except Exception as e:
        print(f'Error: {e}')
    
    time.sleep(5)  # Wait untill the role is created

    return create_role_response['Role']['Arn']