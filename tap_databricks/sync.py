import os
import json
import asyncio
import urllib
from pathlib import Path
from itertools import repeat
from urllib.parse import urljoin

import pytz
import singer
import requests
import pendulum
from singer.bookmarks import write_bookmark, get_bookmark
from pendulum import datetime, period


class DatabricksAuthentication(requests.auth.AuthBase):
    def __init__(self, api_token: str):
        self.api_token = api_token

    def __call__(self, req):
        req.headers.update({"Authorization": " Bearer " + self.api_token})

        return req


class DatabricksClient:
    def __init__(self, auth: DatabricksAuthentication, url="https://dbc-3e90a997-8a9c.cloud.databricks.com/api/2.0/"):
        self._base_url = url
        self._auth = auth
        self._session = None

    @property
    def session(self):
        if not self._session:
            self._session = requests.Session()
            self._session.auth = self._auth
            self._session.headers.update({"Accept": "application/json"})

        return self._session

    def _get(self, path, params=None):
        #url = urljoin(self._base_url, path)
        url = self._base_url + path
        response = self.session.get(url, params=params)
        response.raise_for_status()

        return response

    def cluster(self):
        try:
            clusters = self._get(f"clusters/list")
            return clusters.json()
        except:
            return None

    def job(self):
        try:
            jobs = self._get(f"jobs/list")
            return jobs.json()
        except:
            return None

    def node_type(self):
        try:
            nodes = self._get(f"clusters/list-node-types")
            return nodes.json()
        except:
            return None

class DatabricksSync:
    def __init__(self, client: DatabricksClient, state={}):
        self._client = client
        self._state = state

    @property
    def client(self):
        return self._client

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        singer.write_state(value)
        self._state = value

    def sync(self, stream, schema):
        func = getattr(self, f"sync_{stream}")
        return func(schema)

    async def sync_node_type(self, schema):
        """node type."""
        stream = "node_type"
        loop = asyncio.get_event_loop()

        singer.write_schema(stream, schema.to_dict(), ["node_type_id"])
        node_types = await loop.run_in_executor(None, self.client.node_type)
        extraction_time = singer.utils.now()
        if node_types:
            for node_type in node_types['node_types']:
                singer.write_record(stream, node_type)

       # self.state = singer.write_bookmark(self.state, 'issues', 'start', singer.utils.strftime(extraction_time))

    async  def sync_cluster(self, schema):
        """Clusters."""
        stream = "cluster"
        loop = asyncio.get_event_loop()
        singer.write_schema('cluster', schema.to_dict(), ["cluster_id"])
        clusters = await loop.run_in_executor(None, self.client.cluster)
        if clusters:
            for cluster in clusters['clusters']:
                singer.write_record(stream, cluster)

    async  def sync_job(self, schema):
        """Jobs."""
        stream = "job"
        loop = asyncio.get_event_loop()
        singer.write_schema('job', schema.to_dict(), ["job_id"])
        jobs = await loop.run_in_executor(None, self.client.job)
        if jobs:
            for job in jobs['jobs']:
                singer.write_record(stream, job)



