import boto3
import configparser
from botocore.exceptions import ClientError
import json
import logging
import logging.config
from pathlib import Path
import argparse
import time

##############################################################################

# Setting up logger, Logger properties are defined in logging.ini file
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)

# Loading cluster configurations from cluster.config
config = configparser.ConfigParser()
config.read_file(open('cluster.cfg'))


def create_IAM_role(iam_client):
    """
    Create and IAM_role, Define configuration in cluster.config
    :param iam_client: an IAM service client instance
    :return: True if IAM role created and policy applied successfully.
    """

    role_name = config.get('IAM_ROLE', 'NAME')
    role_description = config.get('IAM_ROLE', 'DESCRIPTION')
    role_policy_arn = config.get('IAM_ROLE', 'POLICY_ARN')

    logging.info(
        f"Creating IAM role with name : {role_name}, "
        f"description : {role_description} and policy : {role_policy_arn}"
    )

    # Creating Role. Policy Documentation reference -
    # https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws
    # -resource-iam-role.html#aws-resource-iam-role--examples
    role_policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": ["redshift.amazonaws.com"]},
                    "Action": ["sts:AssumeRole"]
                }
            ]
        }
    )

    try:
        create_response = iam_client.create_role(
            Path='/',
            RoleName=role_name,
            Description=role_description,
            AssumeRolePolicyDocument=role_policy_document
        )
        logger.debug(
            f"Got response from IAM client for creating role "
            f": {create_response}"
        )
        logger.info(
            f"Role create response code "
            f": {create_response['ResponseMetadata']['HTTPStatusCode']}"
        )
    except Exception as e:
        logger.error(f"Error occured while creating role : {e}")
        return False

    try:
        # Attaching policy using ARN's( Amazon Resource Names )
        policy_response = iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=role_policy_arn
        )
        logger.debug(
            f"Got response from IAM client for applying policy to role "
            f": {policy_response}"
        )
        logger.info(
            f"Attach policy response code "
            f": {policy_response['ResponseMetadata']['HTTPStatusCode']}"
        )
    except Exception as e:
        logger.error(f"Error occured while applying policy : {e}")
        return False

    return True if ((create_response['ResponseMetadata']['HTTPStatusCode'] ==
                     200) and (policy_response['ResponseMetadata']
                               ['HTTPStatusCode'] == 200)) else False


