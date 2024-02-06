import psycopg
import pandas as pd
import hashlib
import datetime
from logger import get_logger

logger = get_logger(__name__)


def get_connection(connection_string: str):
    logger.info(f"Connecting to {connection_string}")
    conn = psycopg.connect(connection_string)
    logger.info(f"Connected to {connection_string}")

    return conn


GET_SNAPSHOT_QUERY = """
    SELECT
        log.log_id,
        s.activity_duration,
        s.customer_id,
        s.project,
        s.subproject,
        log.ticket_number,
        TRIM(BOTH ' -:>,' FROM REPLACE(s.description, log.ticket_number, '')) AS description
    FROM snapshot s
    INNER JOIN log ON s.log_id = log.log_id
    WHERE s.snapshot_id = %s
        AND log.ticket_number IS NOT NULL
        AND LENGTH(log.ticket_number) > 0
        AND UPPER(log.ticket_number) ~ '^[A-Z]'
        AND s.description IS NOT NULL
        AND LENGTH(s.description) > 5
"""


PREPARE_TARGET_TABLE_QUERY = "TRUNCATE TABLE snapshot_changes_ai"


INSERT_TARGET_TABLE_QUERY = """
INSERT INTO snapshot_changes_ai (
    log_id,
    activity_duration,
    customer_id,
    project,
    subproject,
    description,
    control_sum,
    update_date
) VALUES (
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s
)
"""


class SnapshotRepository:
    def __init__(self, conn):
        self.conn = conn

    def read_snapshot(self, snapshot_id: int):
        logger.info("Reading snapshot %s", snapshot_id)
        with self.conn.cursor() as cur:
            cur.execute(GET_SNAPSHOT_QUERY, (snapshot_id,))
            rows = cur.fetchall()
        logger.info(f"Fetched all data: {len(rows)} rows")

        df = pd.DataFrame(rows, columns=["log_id", "activity_duration", "customer_id", "project", "subproject",
                                         "ticket_number", "description"])
        return df

    def write_snapshot_changes(self, df_snapshot, ticket_descriptions: dict):
        logger.info("Preparing target table")
        with self.conn.cursor() as cur:
            cur.execute(PREPARE_TARGET_TABLE_QUERY)
        logger.info("Target table prepared")

        logger.info("Collecting snapshot changes")
        current_date = datetime.datetime.now()
        changes = []
        for row in df_snapshot.itertuples():
            ticket_description = ticket_descriptions.get(row.ticket_number)
            if ticket_description is not None:
                data = (
                    row.log_id,
                    row.activity_duration,
                    row.customer_id,
                    row.project,
                    row.subproject,
                    ticket_description
                )

                changes.append(
                    data + (
                        hashlib.md5("".join(str(d) for d in data).encode("utf-8")).hexdigest(),
                        str(current_date)
                    )
                )
        logger.info(f"Collected {len(changes)} snapshot changes")

        logger.info("Writing changes to target table")
        with self.conn.cursor() as cur:
            cur.executemany(INSERT_TARGET_TABLE_QUERY, changes)
        self.conn.commit()
        logger.info("Changes written to target table")
