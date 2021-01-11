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


def delete_IAM_role(iam_client):
    """
    Delete and IAM role Make sure that you do not have any Amazon EC2
    instances running with the role you are about to delete. Deleting a role
    or instance profile that is associated with a running instance will
    break any applications running on the instance.
    :param iam_client: an IAM service client instance
    :return: True if role deleted successfully.
    """

    role_name = config.get('IAM_ROLE', 'NAME')

    existing_roles = [
        role['RoleName'] for role in iam_client.list_roles()['Roles']
    ]
    if role_name not in existing_roles:
        logger.info(f"Role {role_name} does not exist.")
        return True

    logger.info(f"Processing deleting IAM role : {role_name}")
    try:
        detach_response = iam_client.detach_role_policy(
            RoleName=role_name, PolicyArn=config.get('IAM_ROLE', 'POLICY_ARN')
        )
        logger.debug(
            f"Response for policy detach from IAM role : {detach_response}"
        )
        logger.info(
            f"Detach policy response code "
            f": {detach_response['ResponseMetadata']['HTTPStatusCode']}"
        )
        delete_response = iam_client.delete_role(RoleName=role_name)
        logger.debug(
            f"Response for deleting IAM role : {delete_response}"
        )
        logger.info(
            f"Delete role response code "
            f": {delete_response['ResponseMetadata']['HTTPStatusCode']}"
        )
    except Exception as e:
        logger.error(
            f"Exception occured while deleting role : {e}"
        )
        return False

    return True if ((detach_response['ResponseMetadata']['HTTPStatusCode'] ==
                     200) and (delete_response['ResponseMetadata']
                               ['HTTPStatusCode'] == 200)) else False


def create_cluster(redshift_client, iam_role_arn, vpc_security_group_id):
    """
    Create a Redshift cluster using the IAM role and security group created.
    :param redshift_client: a redshift client instance
    :param iam_role_arn: IAM role arn to give permission to cluster to
    communicate with other AWS service
    :param vpc_security_group_id: vpc group for network setting for cluster
    :return: True if cluster created successfully.
    """

    # Cluster Hardware config
    cluster_type = config.get('DWH', 'DWH_CLUSTER_TYPE')
    node_type = config.get('DWH', 'DWH_NODE_TYPE')
    num_nodes = int(config.get('DWH', 'DWH_NUM_NODES'))

    # Cluster identifiers and credentials
    cluster_identifier = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
    db_name = config.get('DWH', 'DWH_DB')
    database_port = int(config.get('DWH', 'DWH_PORT'))
    master_username = config.get('DWH', 'DWH_DB_USER')
    master_user_password = config.get('DWH', 'DWH_DB_PASSWORD')

    # Cluster adding IAM role
    iam_role = None

    # Security settings
    security_group = config.get('SECURITY_GROUP', 'NAME')

    # Documentation - https://boto3.amazonaws.com/v1/documentation/api
    # /latest/reference/services/redshift.html?highlight=create_cluster
    # #Redshift.Client.create_cluster
    try:
        response = redshift_client.create_cluster(
            DBName=db_name,
            ClusterIdentifier=cluster_identifier,
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=num_nodes,
            MasterUsername=master_username,
            MasterUserPassword=master_user_password,
            VpcSecurityGroupIds=vpc_security_group_id,
            IamRoles=[iam_role_arn]
        )
        logger.debug(
            f"Cluster creation response : {response}"
        )
        logger.info(
            f"Cluster creation response code "
            f": {response['ResponseMetadata']['HTTPStatusCode']} "
        )
    except Exception as e:
        logger.error(
            f"Exception occured while creating cluster : {e}"
        )
        return False

    return response['ResponseMetadata']['HTTPStatusCode'] == 200











































