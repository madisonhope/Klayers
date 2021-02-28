import os
import json
import boto3
from datetime import datetime, timedelta

from boto3.dynamodb.conditions import Key, Attr
from aws_lambda_powertools.logging import Logger
from common.dynamodb import DecimalEncoder, map_keys, query_till_end

logger = Logger()

def last_sunday():
    """
    Get's last Sunday's date, and return in YYYY-MM-DD format
    """
    # last sunday's date
    today = datetime.utcnow()
    idx = (today.weekday() + 1) % 7
    if idx == 0: # today is SUN
        last_sunday = today - timedelta(7)
    else:
        last_sunday = today - timedelta(idx)
    logger.info(f"Extracting data for {last_sunday.isoformat()}")
    
    return last_sunday.isoformat()[:10]


def query_table(region, table):
    """
    Args:
      table: DynamoDB table object to query
      region: region to query on
    returns:
      items: items returned from the query
    """
    
    created_date = last_sunday()

    kwargs = {
        "IndexName": "deployed_in_region",
        "KeyConditionExpression": Key("rgn").eq(region) & Key("dplySts").eq("latest"),
        "FilterExpression": Attr('crtdDt').gt(created_date),
        "ProjectionExpression": "crtdDt, pckg, arn, rgn, pckgVrsn"
    }
    items = query_till_end(table=table, kwargs=kwargs)

    return map_keys(items)


@logger.inject_lambda_context
def main(event, context):
    """
    Gets layers released after last Sunday (assumption is that all releases happen on Monday)
    """

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["DB_NAME"])
    region = event.get("pathParameters",{}).get("region","ap-southeast-1")
    api_response = query_table(table=table, region=region)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type" : "application/json"
        },
        "body": json.dumps(api_response, cls=DecimalEncoder),
    }