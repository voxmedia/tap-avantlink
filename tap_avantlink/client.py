"""REST client handling, including tap-avantlinkStream base class."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Iterable

import csv
from datetime import datetime, timedelta
from io import StringIO
import re
import requests
from requests import Response
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


def sanitize_keys(value):
    if isinstance(value, dict):
        return {
            replace_special_chars(key): sanitize_keys(val) for key, val in value.items()
        }
    if isinstance(value, list):
        return [sanitize_keys(val) for val in value]
    return value


def replace_special_chars(key):
    return re.sub("[ \\-&\\/]", "_", key)


class DayChunkPaginator(BaseAPIPaginator):
    """A paginator that increments days in a date range."""

    def __init__(self, start_date: str, increment: int = 1, *args: Any, **kwargs: Any) -> None:
        super().__init__(start_date)
        self._value = datetime.strptime(start_date, "%Y-%m-%d")
        self._end = datetime.today()
        self._increment = increment

    @property
    def end_date(self):
        """Get the end pagination value.

        Returns:
            End date.
        """
        return self._end

    @property
    def increment(self):
        """Get the paginator increment.

        Returns:
            Increment.
        """
        return self._increment

    def get_next(self, response: Response):
        return self.current_value + timedelta(days=self.increment) if self.has_more(response) else None

    def has_more(self, response: Response) -> bool:
        """Checks if there are more days to process.

        Args:
            response: API response object.

        Returns:
            Boolean flag used to indicate if the endpoint has more pages.
        """
        return self.current_value < self.end_date


class TapAvantlinkStream(RESTStream):
    """tap-avantlink stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://classic.avantlink.com/api.php"
    
    @property
    def next_page_token(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("start_date", "")

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="x-api-key",
            value=self.config.get("auth_token", ""),
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")  # noqa: ERA001
        return headers

    def get_new_paginator(self) -> BaseAPIPaginator:
        return DayChunkPaginator(start_date=self.config.get("start_date"), increment=28)

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {
            "module": "AffiliateReport",
            "affiliate_id": self.config.get("publisher_id"),
            "auth_key": self.config.get("auth_token"),
            "report_id": self.report_id,
            "website_id": "0",
            "merchant_id": "0",
            "output": "csv"
        }
        date_format_str = "%Y-%m-%d %H:%M:%S"
        next_page_date = datetime.strftime(next_page_token, date_format_str)
        if next_page_date:
            params["date_begin"] = next_page_date
            end_datetime = datetime.strptime(next_page_date, date_format_str) + timedelta(days=28)
            params["date_end"] = datetime.strftime(end_datetime, date_format_str)
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        f = StringIO(response.text)
        reader = csv.DictReader(f)
        for row in reader:
            yield row

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        row = sanitize_keys(row)
        return row
