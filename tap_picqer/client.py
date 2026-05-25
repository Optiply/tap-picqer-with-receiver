"""REST client handling, including picqerStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Iterable
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from datetime import datetime
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import BasicAuthenticator


class picqerStream(RESTStream):
    """picqer stream class."""


    # OR use a dynamic url_base:
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        org = self.config.get("org")
        url_base = f"https://{org}.picqer.com/api/v1"
        return url_base

    records_jsonpath = "$[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.next_page"  # Or override `get_next_page_token`.

    @property
    def authenticator(self) -> BasicAuthenticator:
        """Return a new authenticator object."""
        return BasicAuthenticator.create_for_stream(
            self,
            username=self.config.get("api_key"),
            password=None,
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        
        headers["User-Agent"] = "MyPicqerClient (picqer.com/api - support@picqer.com)"
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        # TODO: If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        if self.pagination is False:
            return None
        # Stop pagination on 404 so we don't keep requesting offset=100, 200, ...
        if response.status_code == 404:
            return None
        response_json = self._safe_response_json(response)
        if previous_token is None:
            if not response_json:
                return None
            next_page_token = 100
            return next_page_token
        if not response_json:
            return None
        next_page_token = previous_token + 100
        return next_page_token


    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token and self.pagination:
            params["offset"] = next_page_token
        start_date = self.get_starting_timestamp(context)
        if start_date:
            if self.replication_key == 'updated':
                params["updated_after"] = datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S")
            elif self.replication_key in ['created', 'created_at', 'changed_at']:
                params["sincedate"] = datetime.strftime(start_date, "%Y-%m-%d %H:%M:%S")
        return params

    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> requests.PreparedRequest:
        """Log full request URL and params, then prepare the request."""
        prepared = super().prepare_request(context, next_page_token)
        self.logger.info(
            "HTTP %s %s",
            prepared.method,
            prepared.url,
        )
        return prepared

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.
        """
        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif response.status_code == 404:
            response_json = self._safe_response_json(response)
            if isinstance(response_json, dict) and response_json.get("error_code") == 20:
                # Means that the product doesn't have parts.
                return
            self.logger.warning(
                "Received 404 for endpoint '%s' in stream '%s'. Treating as empty response.",
                self.path,
                self.name,
            )
            return
        elif 400 <= response.status_code < 500:
            response_json = self._safe_response_json(response)
            if (
                isinstance(response_json, dict)
                and response_json.get("error_code") == 31
                and response_json.get("error_message")
            ):
                raise FatalAPIError(response_json["error_message"])
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse records from response body."""
        response_json = self._safe_response_json(response)
        if response_json is None:
            if response.status_code == 404:
                # 404s are treated as empty for optional/unavailable endpoints.
                return
            msg = f"Expected JSON response for path '{self.path}' but got non-JSON payload."
            raise FatalAPIError(msg)
        yield from extract_jsonpath(self.records_jsonpath, input=response_json)

    @staticmethod
    def _safe_response_json(response: requests.Response) -> Optional[Any]:
        """Return response JSON or None if body is not valid JSON."""
        try:
            return response.json()
        except ValueError:
            return None