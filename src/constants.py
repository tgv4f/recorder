from pathlib import Path


ENCODING = "utf-8"

WORK_DIRPATH = Path(__file__).parent.parent
LOGS_DIRPATH = WORK_DIRPATH / "logs"

LOG_FILEPATH = LOGS_DIRPATH / "main.log"


for dirpath in (
    LOGS_DIRPATH,
):
    if not dirpath.exists():
        dirpath.mkdir()
