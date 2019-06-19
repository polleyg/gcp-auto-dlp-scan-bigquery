import base64
import json
import logging
import collections
import time
from google.cloud import bigquery
from google.cloud import dlp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def biqquery_new_table_event_from_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    obj = json.loads(pubsub_message)
    logger.info("Received the following payload: '{}'".format(obj))

    # This shouldn't happen as it will be filtered out by Stackdriver but let's check anyway
    caller_email = obj['protoPayload']['authenticationInfo']['principalEmail']
    if caller_email.endswith('@dlp-api.iam.gserviceaccount.com'):
        logger.error('This is a DLP table creation event. This should not have come through!')
    else:
        service_data = obj['protoPayload']['serviceData']
        table_info = None
        
        # Work out the event
        if 'tableInsertRequest' in service_data:
            resource = service_data['tableInsertRequest']['resource']
            if not resource['view']: # ignore views
                logger.debug("Table insert event")
                table_info = extract_table_and_dataset(resource, 'tableName')
        elif 'jobCompletedEvent' in service_data:
            job_configuration = service_data['jobCompletedEvent']['job']['jobConfiguration']
            if 'load' in job_configuration:
                logger.debug("Load job event")
                table_info = extract_table_and_dataset(job_configuration['load'], 'destinationTable')
            elif 'query' in job_configuration: 
                logger.debug("Query job event")
                table_info = extract_table_and_dataset(job_configuration['query'], 'destinationTable')
                if not is_materialized_query(bigquery.Client(), table_info): # ignore unmaterialized queries
                    logger.info("Ignoring query creation event because it was not materialized by user")
                    table_info = None
            elif 'tableCopy' in job_configuration:
                logger.debug("Table copy event")
                table_info = extract_table_and_dataset(job_configuration['tableCopy'], 'destinationTable')
            else:
                logger.error("I've no idea what this event is. Send help, now!")
        else:
            logger.error("I've no idea what this event is. Send help, now!")
        
        if table_info:
            logger.info("A table with id: '{}' was created in dataset: '{}'".format(table_info.table_id, table_info.dataset_id))
            dlp_all_the_things(table_info)

def is_materialized_query(bq_client, table_info):
    """Works out if the destination table is a hidden dataset/table i.e. a normal query
    Args:
         bq_client: BigQuery client
         table_info: encapsulates the table id and dataset id
    """

    datasets = list(bq_client.list_datasets())
    project = bq_client.project

    if datasets:
        for dataset in datasets:
            if table_info.dataset_id.lower() == dataset.dataset_id.lower():
                return True
    return False

def extract_table_and_dataset(payload, key):
    TableInfo = collections.namedtuple('TableInfo', ['project_id', 'dataset_id', 'table_id'])
    table_info = TableInfo(payload[key]['projectId'], payload[key]['datasetId'], payload[key]['tableId'])
    return table_info

def dlp_all_the_things(table_info):
    project = table_info.project_id
    dataset = table_info.dataset_id
    table = table_info.table_id
    
    dlp_client = dlp.DlpServiceClient()
    logger.info("DLP'ing all the things on '{}.{}.{}'".format(project, dataset, table))
    
    inspect_config = {
      'info_types': [],
      'min_likelihood': 'POSSIBLE'
    }

    storage_config = {
        'big_query_options': {
            'table_reference': {
                'project_id': project,
                'dataset_id': dataset,
                'table_id': table,
            }
        }
    }

    parent = dlp_client.project_path(project)

    actions = [{
        'save_findings': {
            'output_config': {
                'table': {
                    'project_id': project,
                    'dataset_id': dataset,
                    'table_id': '{}_dlp_scan_results_{}'.format(table, int(round(time.time() * 1000))),
                }
            }
        }
    }]

    inspect_job = {
        'inspect_config': inspect_config,
        'storage_config': storage_config,
        'actions': actions,
    }

    dlp_client.create_dlp_job(parent, inspect_job=inspect_job)