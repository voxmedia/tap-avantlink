"""tap-avantlink tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_avantlink import streams


class TapAvantlink(Tap):
    """tap-avantlink tap class."""

    name = "tap-avantlink"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "publisher_id",
            th.StringType,
            required=True,
            description="Publisher ID",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        )
    ).to_dict()

    def discover_streams(self) -> list[streams.TapAvantlinkStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.SalesHitsStream(self),
            streams.SalesStream(self),
        ]


if __name__ == "__main__":
    TapAvantlink.cli()
