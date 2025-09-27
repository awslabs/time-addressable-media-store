from typing import Optional

from pydantic import Field
from schema import Webhookget


class Webhookfull(Webhookget):
    api_key_value: Optional[str] = Field(
        None, description="The value that the HTTP header 'api_key_name' will be set to"
    )
