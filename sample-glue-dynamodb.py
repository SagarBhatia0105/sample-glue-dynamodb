import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('data-table')
sc = SparkContext()
glueContext = GlueContext(sc)
dynamodb = boto3.client('dynamodb')
spark = glueContext.spark_session
logger = glueContext.get_logger()
df = spark.read.option("header", True).csv('s3://sagar-csv-data-bucket/sample-data-1m.csv')
logger.info('Loading data to DynamoDB')

#Using toLocalIterator()
dataCollect=df.rdd.toLocalIterator()

with table.batch_writer() as batch:
    for row in dataCollect:
        batch.put_item(
            Item={
                'accountId': row['accountId'],
                'date': row['date'],
                'value': row['value']
            }
        )

logger.info('Data loaded to DynamoDB')
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()