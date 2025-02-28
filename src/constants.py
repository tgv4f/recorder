from pathlib import Path

import typing


ENCODING: typing.Final = "utf-8"

WORK_DIRPATH: typing.Final = Path(__file__).parent.parent
LOGS_DIRPATH: typing.Final = WORK_DIRPATH / "logs"

LOG_FILEPATH: typing.Final = LOGS_DIRPATH / "main.log"


for dirpath in (
    LOGS_DIRPATH,
):
    if not dirpath.exists():
        dirpath.mkdir()
