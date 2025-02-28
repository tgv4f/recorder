from pydantic import BaseModel, Field, field_validator

import re
import sys
import yaml
import logging
import typing_extensions

from src import constants


USERNAME_PATTERN = re.compile(r"[@+\s]")


CONFIG_FILEPATH = constants.WORK_DIRPATH / (
    sys.argv[sys.argv.index("--config") + 1]
    if "--config" in sys.argv
    else
    "config.yml"
)


class SessionConfig(BaseModel):
    name: str
    api_id: int
    api_hash: str
    phone_number: str


class Config(BaseModel):
    session: SessionConfig
    control_chat_id: int | str
    default_listen_chat_id: int | str | None = Field(default=None)
    send_to_chat_id: int | str | None = Field(default=None)
    console_log_level: int = Field(default=logging.INFO)
    file_log_level: int = Field(default=logging.DEBUG)

    @field_validator("control_chat_id", "default_listen_chat_id", "send_to_chat_id", mode="after")
    @classmethod
    def validate_chat_id(cls, value: int | str | None) -> int | str | None:
        if isinstance(value, str):
            return USERNAME_PATTERN.sub("", value.lower())

        return value

    @field_validator("console_log_level", "file_log_level", mode="before")
    @classmethod
    def parse_log_level(cls, value: str) -> int:
        value = value.upper()

        if value not in logging._nameToLevel:
            raise ValueError(f"Invalid log level: {value}")

        return logging._nameToLevel[value]

    @classmethod
    def load(cls) -> typing_extensions.Self:
        with CONFIG_FILEPATH.open("r", encoding=constants.ENCODING) as file:
            return cls.model_validate(
                obj = yaml.load(
                    stream = file,
                    Loader = yaml.Loader
                )
            )

    def save(self) -> None:
        with CONFIG_FILEPATH.open("w", encoding=constants.ENCODING) as file:
            yaml.dump(
                data = self.model_dump(),
                stream = file,
                indent = 2,
                allow_unicode = True,
                sort_keys = False
            )


config = Config.load()
