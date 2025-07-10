"""Authentication handling for tap-tilroy."""

from __future__ import annotations

from singer_sdk.authenticators import APIKeyAuthenticator


class TilroyAuthenticator(APIKeyAuthenticator):
    """Authenticator class for Tilroy."""

    @classmethod
    def create_for_stream(cls, stream) -> TilroyAuthenticator:
        """Create an authenticator for a specific Singer stream.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator.
        """
        return cls(
            stream=stream,
            key="Tilroy-Api-Key",
            value=stream.config["tilroy_api_key"],
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = super().http_headers
        headers["x-api-key"] = self.stream.config["x_api_key"]
        headers["Content-Type"] = "application/json"
        return headers
