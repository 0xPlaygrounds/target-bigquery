import json
import os
import sys

import singer
from google.api_core import exceptions
from google.cloud import bigquery
from google.cloud.bigquery import Dataset
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import DatasetReference
from google.oauth2.service_account import Credentials


logger = singer.get_logger()


def emit_state(state):
    """
    Given a state, writes the state to a state file (e.g., state.json.tmp)

    :param state, State: state with bookmarks dictionary
    """
    if state is not None:
        line = json.dumps(state)
        logger.debug("Emitting state {}".format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

        if os.environ.get("TARGET_BIGQUERY_STATE_FILE", None):
            fn = os.environ.get("TARGET_BIGQUERY_STATE_FILE", None)
            with open(fn, "a") as f:
                f.write("{}\n".format(line))


def get_client_from_credentials(creds_dict, project_id, location):
    creds = Credentials.from_service_account_info(creds_dict)
    return bigquery.Client(credentials=creds, project=project_id, location=location)


def ensure_dataset(project_id, dataset_id, location, config):
    """
    Given a project id, dataset id and location, creates BigQuery dataset if not exists

    https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html

    :param project_id, str: GCP project id from target config file. Passed to bigquery.Client().
    :param dataset_id, str: BigQuery dataset id from target config file.
    :param location, str: location for the dataset (US). Passed to bigquery.Client().
    :return: client (BigQuery Client Object) and Dataset (BigQuery dataset)
    """

    if creds := config.get("service_credentials"):
        client = get_client_from_credentials(project_id, location, creds)
    # elif creds := config.get("web_credentials"):
    #     client = get_client_from_web(project_id, location, creds)
    else:
        client = bigquery.Client(project=project_id, location=location)

    dataset_ref = DatasetReference(project_id, dataset_id)

    try:
        dataset = client.get_dataset(dataset_ref)
        return client, dataset

    except NotFound:
        try:
            client.create_dataset(dataset_ref)
        except exceptions.GoogleAPICallError as e:
            logger.critical(f"unable to create dataset {dataset_id} in project {project_id}; Exception {e}")
            return 2  # sys.exit(2)

        return client, Dataset(dataset_ref)
