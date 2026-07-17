from enum import StrEnum
from typing import Optional

from pydantic import Field
from schema import Webhookget


class Webhookfull(Webhookget):
    api_key_value: Optional[str] = Field(
        None, description="The value that the HTTP header 'api_key_name' will be set to"
    )


class SourcesSortBy(StrEnum):
    """Parameter to sort Sources by."""

    created = "created"
    updated = "updated"
    label = "label"


class FlowsSortBy(StrEnum):
    """Parameter to sort Flows by."""

    created = "created"
    metadata_updated = "metadata_updated"
    label = "label"


class DeleteRequestsSortBy(StrEnum):
    """Parameter to sort Flow Delete Requests by."""

    created = "created"
    expiry = "expiry"
