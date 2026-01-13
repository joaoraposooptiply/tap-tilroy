"""REST client handling, including TilroyStream base class."""

from __future__ import annotations

import json
import typing as t
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse

import requests
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import RESTStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class TilroyPaginator(BaseAPIPaginator[int]):
    """Simple page-based paginator for Tilroy API."""

    def __init__(self, start_value: int = 1) -> None:
        """Initialize the paginator."""
        super().__init__(start_value)
        self._page = start_value

    def get_next(self, response: requests.Response) -> int | None:
        """Get the next page number from response headers.

        Args:
            response: The HTTP response object.

        Returns:
            The next page number, or None if no more pages.
        """
        try:
            current_page = int(response.headers.get("X-Paging-CurrentPage", 1))
            total_pages = int(response.headers.get("X-Paging-PageCount", 1))

            if current_page < total_pages:
                return current_page + 1
        except (ValueError, TypeError):
            pass

        return None


class TilroyStream(RESTStream[int]):
    """Base Tilroy stream class with common functionality."""

    # Class-level configuration
    records_jsonpath: str = "$[*]"
    default_count: int = 100
    
    # Replication settings (can be overridden by subclasses)
    replication_key: str | None = None
    replication_method: str = "FULL_TABLE"

    @property
    def url_base(self) -> str:
        """Return the base URL for the API."""
        return self.config["api_url"]

    @property
    def http_headers(self) -> dict[str, str]:
        """Return headers for HTTP requests.

        Returns:
            Dictionary of HTTP headers including authentication.
        """
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Tilroy-Api-Key": self.config["tilroy_api_key"],
            "x-api-key": self.config["x_api_key"],
        }

    def get_new_paginator(self) -> TilroyPaginator:
        """Return a new paginator instance.

        Returns:
            A TilroyPaginator starting at page 1.
        """
        return TilroyPaginator(start_value=1)

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Return URL query parameters for the request.

        Args:
            context: Stream partition context.
            next_page_token: Page number for pagination.

        Returns:
            Dictionary of URL parameters.
        """
        return {
            "count": self.default_count,
            "page": next_page_token or 1,
        }

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the API response.

        Args:
            response: The HTTP response object.

        Yields:
            Parsed records from the response.
        """
        try:
            data = response.json()
        except json.JSONDecodeError:
            self.logger.error(f"Failed to decode JSON response: {response.text[:500]}")
            return

        # Check for API error response
        if isinstance(data, dict) and "code" in data and "message" in data:
            self.logger.error(f"API error: {data.get('message', 'Unknown error')}")
            return

        yield from extract_jsonpath(self.records_jsonpath, input=data)

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process a record.

        Handles NA values and type conversions.

        Args:
            row: The record to process.
            context: Stream partition context.

        Returns:
            The processed record, or None to skip.
        """
        if not row:
            return None

        for key, value in row.items():
            if value is None:
                continue

            # Handle NA string values
            if isinstance(value, str) and value.upper() in ("NA", "N/A", "NULL", "NONE", ""):
                row[key] = None
                continue

            # Convert string numbers to float for number fields
            if isinstance(value, str):
                field_schema = self.schema.get("properties", {}).get(key, {})
                field_types = field_schema.get("type", [])
                if isinstance(field_types, str):
                    field_types = [field_types]

                if "number" in field_types:
                    try:
                        row[key] = float(value)
                    except ValueError:
                        pass  # Keep as string if conversion fails

        return row

    def _stringify_nested_objects(self, data: dict) -> dict:
        """Convert nested objects to JSON strings for CSV compatibility.

        Args:
            data: The record with potential nested objects.

        Returns:
            Record with nested objects converted to JSON strings.
        """
        result = {}
        for key, value in data.items():
            if isinstance(value, dict):
                result[key] = json.dumps(value)
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                result[key] = json.dumps(value)
            else:
                result[key] = value
        return result


class DateFilteredStream(TilroyStream):
    """Base class for streams with date-based incremental filtering."""

    # Date parameter name (override in subclasses if different)
    date_param_name: str = "dateFrom"

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters including date filter.

        Args:
            context: Stream partition context.
            next_page_token: Page number for pagination.

        Returns:
            Dictionary of URL parameters with date filtering.
        """
        params = super().get_url_params(context, next_page_token)

        # Get start date from bookmark or config
        start_date = self._get_start_date(context)
        params[self.date_param_name] = start_date.strftime("%Y-%m-%d")

        return params

    def _get_start_date(self, context: Context | None) -> datetime:
        """Determine the start date for filtering.

        Args:
            context: Stream partition context.

        Returns:
            The start date for the query.
        """
        # Try to get from bookmark
        bookmark_date = self.get_starting_timestamp(context)

        if bookmark_date:
            # Go back 1 day to avoid missing records at boundary
            if hasattr(bookmark_date, "date"):
                date_only = bookmark_date.date()
            else:
                date_only = bookmark_date
            return datetime.combine(date_only - timedelta(days=1), datetime.min.time())

        # Fall back to config start_date
        config_start = self.config.get("start_date", "2010-01-01T00:00:00Z")
        date_part = config_start.split("T")[0]
        return datetime.strptime(date_part, "%Y-%m-%d")


class LastIdPaginatedStream(DateFilteredStream):
    """Base class for streams using lastId cursor pagination."""

    # Override in subclasses
    last_id_field: str = "tilroyId"
    last_id_param: str = "lastId"

    def get_new_paginator(self) -> None:
        """Return None - we handle pagination manually with lastId.

        Returns:
            None (pagination handled in request_records).
        """
        return None

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: str | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters for lastId pagination.

        Args:
            context: Stream partition context.
            next_page_token: The lastId from previous response.

        Returns:
            Dictionary of URL parameters.
        """
        params = {
            "count": self.default_count,
            self.date_param_name: self._get_start_date(context).strftime("%Y-%m-%d"),
        }

        if next_page_token:
            params[self.last_id_param] = next_page_token

        return params

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request records using lastId pagination.

        Args:
            context: Stream partition context.

        Yields:
            Records from the API.
        """
        last_id: str | None = None

        while True:
            params = self.get_url_params(context, last_id)
            
            self.logger.info(f"[{self.name}] Requesting with params: {params}")

            prepared_request = self.build_prepared_request(
                method="GET",
                url=self.get_url(context),
                params=params,
                headers=self.http_headers,
            )

            response = self._request(prepared_request, context)

            if response.status_code != 200:
                self.logger.error(f"[{self.name}] API error {response.status_code}: {response.text[:500]}")
                break

            records = list(self.parse_response(response))
            self.logger.info(f"[{self.name}] Retrieved {len(records)} records")

            if not records:
                break

            for record in records:
                processed = self.post_process(record, context)
                if processed:
                    yield processed

            # Get lastId for next page
            last_record = records[-1]
            if isinstance(last_record, dict) and self.last_id_field in last_record:
                last_id = str(last_record[self.last_id_field])
            else:
                self.logger.warning(f"[{self.name}] Could not find {self.last_id_field} in last record")
                break

            # If we got fewer than count, we're done
            if len(records) < self.default_count:
                break

        self.logger.info(f"[{self.name}] Pagination complete")


class DynamicRoutingStream(DateFilteredStream):
    """Base class for streams that switch between historical and incremental endpoints.

    Uses historical_path for first sync (no state) and incremental_path for subsequent syncs.
    """

    historical_path: str | None = None
    incremental_path: str | None = None

    @property
    def path(self) -> str:
        """Return the appropriate path based on sync state.

        Returns:
            The API path to use for this sync.
        """
        if self._has_existing_state():
            selected = self.incremental_path
            sync_type = "INCREMENTAL"
        else:
            selected = self.historical_path
            sync_type = "HISTORICAL"

        self.logger.info(f"[{self.name}] Using {sync_type} path: {selected}")
        return selected or ""

    def _has_existing_state(self) -> bool:
        """Check if the stream has existing state/bookmarks.

        Returns:
            True if there's existing state for this stream.
        """
        if not self.replication_key:
            return False

        state = self.tap_state or {}

        # Handle both Singer state formats
        if "value" in state:
            bookmarks = state.get("value", {}).get("bookmarks", {})
        else:
            bookmarks = state.get("bookmarks", {})

        stream_state = bookmarks.get(self.name, {})
        return bool(stream_state.get("replication_key_value"))
