"""REST client handling, including TilroyStream base class."""

from __future__ import annotations

import json
import time
import typing as t
from datetime import datetime, timedelta

import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
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

    # Retry configuration — SDK uses exponential backoff on RetriableAPIError
    backoff_max_tries = 8

    def validate_response(self, response: requests.Response) -> None:
        """Handle rate limits (429) and server errors (5xx) with retries."""
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            wait = int(retry_after) if retry_after else 30
            self.logger.warning(
                f">>> [{self.name}] RATE LIMITED (429) — waiting {wait}s before retry <<<"
            )
            time.sleep(wait)
            raise RetriableAPIError(
                f"Rate limited (429): {response.text[:200]}", response
            )

        if response.status_code == 504:
            self.logger.warning(
                f">>> [{self.name}] GATEWAY TIMEOUT (504) — retrying <<<"
            )
            raise RetriableAPIError(
                f"Gateway Timeout (504)", response
            )

        if response.status_code >= 500:
            self.logger.warning(
                f">>> [{self.name}] SERVER ERROR ({response.status_code}) — retrying <<<"
            )
            raise RetriableAPIError(
                f"Server error ({response.status_code}): {response.text[:200]}",
                response,
            )

        if response.status_code == 408:
            self.logger.warning(
                f">>> [{self.name}] REQUEST TIMEOUT (408) — retrying <<<"
            )
            raise RetriableAPIError(f"Request Timeout (408)", response)

        if 400 <= response.status_code < 500:
            raise FatalAPIError(
                f"Client error ({response.status_code}): {response.text[:200]}",
                response,
            )

    def _request_with_backoff(
        self,
        prepared: requests.PreparedRequest,
        context: Context | None,
    ) -> requests.Response:
        """Execute request with SDK's exponential backoff on retriable errors.

        Use this instead of self._request() in custom request_records methods
        to get automatic retry on 429, 504, and connection errors.
        """
        decorated = self.request_decorator(self._request)
        return decorated(prepared, context)

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

    # Fields that should be kept as integers (not floats)
    _integer_fields: frozenset = frozenset({
        "tilroyId", "sourceId", "number", "tilroy_id", "source_id",
        "legalEntityId", "tenantId", "shopNumber", "shop_number",
        "idTilroy", "idTenant", "idSource",  # Transfer API field names
    })

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

            # Convert floats to int for ID/number fields (avoid .0 suffix)
            if isinstance(value, float) and key in self._integer_fields:
                if value == int(value):  # Only if it's a whole number
                    row[key] = int(value)
                continue

            # Convert string numbers to appropriate type for number fields
            if isinstance(value, str):
                field_schema = self.schema.get("properties", {}).get(key, {})
                field_types = field_schema.get("type", [])
                if isinstance(field_types, str):
                    field_types = [field_types]

                if "number" in field_types or "integer" in field_types:
                    try:
                        num_value = float(value)
                        # Keep as int if it's a whole number and an ID field
                        if key in self._integer_fields and num_value == int(num_value):
                            row[key] = int(num_value)
                        else:
                            row[key] = num_value
                    except ValueError:
                        pass  # Keep as string if conversion fails

        return row

    def _flatten_shop(self, row: dict, shop_field: str = "shop", prefix: str = "shop") -> dict:
        """Flatten a nested shop object into separate fields.

        Args:
            row: The record containing the shop object.
            shop_field: The name of the shop field in the record.
            prefix: Prefix for the flattened field names.

        Returns:
            The record with flattened shop fields added.
        """
        shop = row.get(shop_field)
        if isinstance(shop, dict):
            row[f"{prefix}_tilroy_id"] = shop.get("tilroyId") or shop.get("idTilroy")
            row[f"{prefix}_number"] = shop.get("number") or shop.get("shopNumber")
            row[f"{prefix}_name"] = shop.get("name")
            row[f"{prefix}_source_id"] = shop.get("sourceId") or shop.get("idSource")
        else:
            row[f"{prefix}_tilroy_id"] = None
            row[f"{prefix}_number"] = None
            row[f"{prefix}_name"] = None
            row[f"{prefix}_source_id"] = None
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


class DateWindowedStream(DateFilteredStream):
    """Base class for streams that need date windowing to avoid API timeouts.

    Breaks large date ranges into smaller chunks (windows) to prevent the API
    from timing out on heavy queries. Supports both page-based and lastId-based
    pagination.
    """

    # Size of each date window in days (can be overridden)
    date_window_days: int = 7
    # Whether to use dateTo parameter (some APIs require it)
    use_date_to: bool = True
    date_to_param_name: str = "dateTo"
    # Whether to use lastId pagination instead of page pagination
    use_last_id_pagination: bool = False
    last_id_field: str = "idTilroySale"
    last_id_param: str = "lastId"

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request records using date windowing with pagination.

        Breaks the date range into windows of `date_window_days` size,
        and paginates through each window before moving to the next.

        Args:
            context: Stream partition context.

        Yields:
            Records from the API.
        """
        start_date = self._get_start_date(context)
        end_date = datetime.now()

        pagination_type = "lastId" if self.use_last_id_pagination else "page"
        self.logger.info(
            f"[{self.name}] Starting date-windowed sync from {start_date.date()} "
            f"to {end_date.date()} with {self.date_window_days}-day windows "
            f"using {pagination_type} pagination"
        )

        window_start = start_date
        total_records = 0

        while window_start < end_date:
            # Calculate window end (don't exceed today)
            window_end = min(
                window_start + timedelta(days=self.date_window_days),
                end_date
            )

            self.logger.info(
                f"[{self.name}] Processing window: {window_start.date()} to {window_end.date()}"
            )

            # Paginate through this window
            page = 1
            last_id: str | None = None
            window_records = 0

            while True:
                params = self._get_window_params(window_start, window_end, page, last_id)

                self.logger.debug(f"[{self.name}] Window request params: {params}")

                prepared_request = self.build_prepared_request(
                    method="GET",
                    url=self.get_url(context),
                    params=params,
                    headers=self.http_headers,
                )

                try:
                    response = self._request_with_backoff(prepared_request, context)
                except Exception as e:
                    self.logger.error(f"[{self.name}] Request failed: {e}")
                    break

                if response.status_code != 200:
                    self.logger.error(
                        f"[{self.name}] API error {response.status_code}: {response.text[:500]}"
                    )
                    break

                records = list(self.parse_response(response))
                page_count = len(records)
                window_records += page_count

                self.logger.info(
                    f"[{self.name}] Window {window_start.date()}-{window_end.date()} "
                    f"page {page}: {page_count} records"
                )

                for record in records:
                    processed = self.post_process(record, context)
                    if processed:
                        total_records += 1
                        yield processed

                # Check if more pages in this window
                if self.use_last_id_pagination:
                    # lastId pagination - get ID from last record
                    if page_count < self.default_count:
                        break  # No more records
                    if records:
                        last_record = records[-1]
                        if isinstance(last_record, dict) and self.last_id_field in last_record:
                            last_id = str(last_record[self.last_id_field])
                            page += 1
                        else:
                            self.logger.warning(
                                f"[{self.name}] Could not find {self.last_id_field} in last record"
                            )
                            break
                    else:
                        break
                else:
                    # Page-based pagination - check headers
                    try:
                        current_page = int(response.headers.get("X-Paging-CurrentPage", 1))
                        total_pages = int(response.headers.get("X-Paging-PageCount", 1))
                        if current_page >= total_pages:
                            break
                        page += 1
                    except (ValueError, TypeError):
                        # If we got fewer than count, assume no more pages
                        if page_count < self.default_count:
                            break
                        page += 1

            self.logger.info(
                f"[{self.name}] Window {window_start.date()}-{window_end.date()} "
                f"complete: {window_records} records"
            )

            # Move to next window
            window_start = window_end

        self.logger.info(f"[{self.name}] Date-windowed sync complete: {total_records} total records")

    def _get_window_params(
        self,
        window_start: datetime,
        window_end: datetime,
        page: int,
        last_id: str | None = None,
    ) -> dict[str, t.Any]:
        """Get URL parameters for a specific date window and page.

        Args:
            window_start: Start of the date window.
            window_end: End of the date window.
            page: Current page number (used for page-based pagination).
            last_id: Last record ID (used for lastId pagination).

        Returns:
            Dictionary of URL parameters.
        """
        params: dict[str, t.Any] = {
            "count": self.default_count,
            self.date_param_name: window_start.strftime("%Y-%m-%d"),
        }

        if self.use_date_to:
            params[self.date_to_param_name] = window_end.strftime("%Y-%m-%d")

        if self.use_last_id_pagination:
            # Use lastId for pagination (0 to start, then actual ID)
            params[self.last_id_param] = last_id if last_id else "0"
        else:
            params["page"] = page

        return params


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
