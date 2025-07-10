"""REST client handling, including TilroyStream base class."""

from __future__ import annotations

import decimal
import typing as t
from functools import cached_property
from importlib import resources
from pathlib import Path
from typing import Any, Callable, Iterable

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator, JSONPathPaginator
from singer_sdk.streams import RESTStream
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.authenticators import APIKeyAuthenticator

from tap_tilroy.auth import TilroyAuthenticator
import requests
import json
import http.client

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Auth, Context


# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]


class TilroyStream(RESTStream):
    """Tilroy stream class."""

    default_count = 100  # Default count per page

    @property
    def url_base(self) -> str:
        return self.config["api_url"]

    def flatten_record(self, record: dict, parent_key: str = "", sep: str = ".") -> dict:
        """Flatten a nested dictionary by concatenating nested keys with a separator.

        Args:
            record: The record to flatten
            parent_key: The parent key for nested dictionaries
            sep: The separator to use between nested keys

        Returns:
            A flattened dictionary
        """
        items = []
        for key, value in record.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            
            if isinstance(value, dict):
                items.extend(self.flatten_record(value, new_key, sep=sep).items())
            elif isinstance(value, list):
                # For arrays, we'll keep them as is
                items.append((new_key, value))
            else:
                items.append((new_key, value))
                
        return dict(items)

    def post_process(self, row: dict, context: t.Optional[dict] = None) -> dict:
        """Post-process a record after it has been fetched.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            The processed record.
        """
        return self.flatten_record(row)

    def get_headers(self, context: t.Optional[dict] = None) -> dict:
        """Get headers for the request.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            Dictionary of headers.
        """
        return {
            "Content-Type": "application/json",
            "Tilroy-Api-Key": self.config["tilroy_api_key"],
            "x-api-key": self.config["x_api_key"],
        }

    def get_url_params(self, context: t.Optional[dict], page: int) -> dict:
        """Get URL parameters for the request.

        Args:
            context: Stream partition or context dictionary.
            page: The current page number.

        Returns:
            Dictionary of URL parameters.
        """
        params = {
            "count": self.default_count,
            "page": page,
        }
        if context:
            params.update(context)
        return params

    def request_records(self, context: t.Optional[dict]) -> t.Iterator[dict]:
        """Request records from the stream.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Records from the stream.
        """
        page = 1
        while True:
            # Use get_url_params to build the params dict
            params = self.get_url_params(context, page)
            url = f"{self.url_base}{self.path}"
            headers = self.get_headers(context)
            # Debug log to print the final URL and parameters
            self.logger.info(f"Request URL: {url}")
            self.logger.info(f"Request params: {params}")
            try:
                # Use http.client for the request
                conn = http.client.HTTPSConnection("api.tilroy.com")
                query_string = "&".join([f"{k}={v}" for k, v in params.items()])
                conn.request("GET", f"{self.path}?{query_string}", "", headers)
                response = conn.getresponse()
                data = response.read().decode("utf-8")
                # Parse the response
                data_json = json.loads(data)
                # Extract records using jsonpath
                records = list(extract_jsonpath(self.records_jsonpath, data_json))
                if not records:  # If no records returned, we've reached the end
                    break
                for record in records:
                    yield record
                # If we got exactly default_count records, there might be more pages
                if len(records) < self.default_count:
                    break
                page += 1  # Move to next page
            except Exception as e:
                self.logger.error(f"Error fetching records: {str(e)}")
                raise
