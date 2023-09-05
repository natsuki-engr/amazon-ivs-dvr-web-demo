'''
recording-started.jsonが保存されたタイミングで配信IDを追加したrecording-started-latest.jsonをs3に保存
'''
import json
from logging import getLogger, INFO
import boto3

logger = getLogger(__name__)
logger.setLevel(INFO)
s3 = boto3.resource('s3')
ivs = boto3.client('ivs')

def lambda_handler(event, context):
    print("============ logger.info の出力 ============")
    logger.info(json.dumps(event))
    key = event['Records'][0]['s3']['object']['key']
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    s3_recording_key_prefix = key.split('/events')[0]
    logger.info("key")
    logger.info(key)
    logger.info("bucket_name")
    logger.info(bucket_name)
    logger.info("s3_recording_key_prefix")
    logger.info(s3_recording_key_prefix)
    
    try:
        bucket = s3.Bucket(bucket_name)
        obj = bucket.Object(key)
        response = obj.get()    
        logger.info("bookmark 02")
        body = response['Body'].read()
        logger.info("body")
        logger.info(body)
        metadata = json.loads(body)
        logger.info('metadata')
        logger.info(metadata)
        relative_path = metadata['media']['hls']['path']
        logger.info("relative_path")
        logger.info(relative_path)
        absolute_path = s3_recording_key_prefix + '/' + relative_path
        logger.info('absolute_path')
        logger.info(absolute_path)
        metadata['media']['hls']['path'] = absolute_path
        
        stream = ivs.get_stream(channelArn=metadata['channel_arn'])
        logger.info('stream')
        logger.info(stream)
        metadata.streamId = stream.stream_id if stream else ''
        s3.put_object(Body=json.dumps(metadata), Bucket=bucket, Key='recording-started-latest.json')
    except Exception as error:
        logger.error("[error] " + error)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
        }),
    }
