"""Gong Authentication."""

import base64
from singer_sdk.authenticators import SimpleAuthenticator


class GongAuthenticator(SimpleAuthenticator):
    """Authenticator class for Gong."""

    @classmethod
    def create_for_stream(cls, stream) -> "GongAuthenticator":
        # Authentication
        raw_credentials = f"{stream.config['access_key']}:{stream.config['access_key_secret']}"
        auth_token = base64.b64encode(raw_credentials.encode()).decode("ascii")
        return cls(
            stream=stream,
            auth_headers={
                "Authorization": f"Basic {auth_token}"
            }
        )
