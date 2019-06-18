import base64
import json
import logging
import collections
from google.cloud import bigquery

def biqquery_new_table_event_from_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    obj = json.loads(pubsub_message)
    logger.debug("Received the following payload: '{}'".format(obj))
    service_data = obj['protoPayload']['serviceData']
    
    # Work out the event (edge cases: views, queries (temp tables), DLP creating the table again (endless loop))
    # Always have ['protoPayload']['serviceData']
    if 'tableInsertRequest' in service_data:
        # Always have 'view'
        resource = service_data['tableInsertRequest']['resource']
        if resource['view']:
            # Edge case 1: new view
            logger.info("Ignoring view creation event")
        else:
            # New table
            table_id = resource['tableName']['tableId']
            dataset_id = resource['tableName']['datasetId']
            logger.info("A table with id: '{}' was created in dataset: '{}'. Will now scan using DLP.".format(table_id, dataset_id))
    elif 'jobCompletedEvent' in service_data:
        job_configuration = service_data['jobCompletedEvent']['job']['jobConfiguration']
        if 'load' in job_configuration:
            # Load job
            logger.debug("Load job event")
            table_info = extract_table_and_dataset(job_configuration['load'])
            logger.info("A table with id: '{}' was created in dataset: '{}'. Will now scan using DLP.".format(table_info.table_id, table_info.dataset_id))
        elif 'query' in job_configuration: 
            # Query job
            logger.debug("Query job event")
            destinationTable = job_configuration['query']['destinationTable']
            dataset_id = destinationTable['datasetId']
            table_id = destinationTable['tableId']
            if is_materialized_query(bigquery.Client(), dataset_id, table_id):
                # Query with destination table saved
                logger.info("A table with id: '{}' was created in dataset: '{}'. Will now scan using DLP.".format(table_id, dataset_id))
            else:
                # Edge case 2: a query written to a temp/hidden table
                logger.info("Ignoring query creation event because it was not materialized by user")
        elif 'tableCopy' in job_configuration:
            # Copy job
            logger.debug("Table copy event")
            destinationTable = job_configuration['tableCopy']['destinationTable']
            dataset_id = destinationTable['datasetId']
            table_id = destinationTable['tableId']
            logger.info("A table with id: '{}' was created in dataset: '{}'. Will now scan using DLP.".format(table_id, dataset_id))
        else:
        	logger.error("I've no idea what this event is. Send help!")
    else:
	    logger.error("I've no idea what this event is. Send help!")

def is_materialized_query(bq_client, dataset_id, table_id):
    """Works out if the destination table is a hidden dataset/table i.e. a normal query
    Args:
         bq_client: BigQuery client
         dataset_id: the dataset id of the incoming event
         table_id: the table id of the incoming event
    """
    datasets = list(bq_client.list_datasets())
    project = bq_client.project

    if datasets:
        for dataset in datasets:
            if dataset_id.lower() == dataset.dataset_id.lower():
                return True
    return False

def extract_table_and_dataset(payload):
    destination = payload['destinationTable']
    TableInfo = collections.namedtuple('TableInfo', ['table_id', 'dataset_id'])
    table_info = TableInfo(destination['tableId'], destination['datasetId'])
    return table_info