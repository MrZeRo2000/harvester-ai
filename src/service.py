from openai import OpenAI, RateLimitError
from dataclasses import dataclass
from logger import get_logger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential, RetryError,
)
from repository import get_connection, SnapshotRepository

# noinspection PyPep8
OPENAI_REQUEST = 'Write a concise summary for diverse entries, eliminating duplicates, URLs, and emojis, ensuring clarity and coherence within 170 characters without generating new entries'

logger = get_logger(__name__)


@dataclass
class Params:
    connection_string: str
    openai_api_key: str
    snapshot_id: int


class ProcessingService:
    def __init__(self, params: Params):
        self.params = params
        self.openai_client = OpenAI(api_key=params.openai_api_key)

    def execute(self):
        logger.info("Executing Processing Service")
        conn = get_connection(self.params.connection_string)

        snapshot_repository = SnapshotRepository(conn)
        df_snapshot = snapshot_repository.read_snapshot(self.params.snapshot_id)

        ticket_descriptions = self.process_snapshot(df_snapshot)

        snapshot_repository.write_snapshot_changes(df_snapshot, ticket_descriptions)

    def process_snapshot(self, df_snapshot) -> dict:
        result = {}

        df_descriptions = df_snapshot.groupby("ticket_number").agg({'description': lambda s: list(set(s))})
        df_descriptions_to_process = df_descriptions[df_descriptions['description'].str.len() > 1]
        logger.info(
            f"Total number of items: {len(df_descriptions)}, items to process: {len(df_descriptions_to_process)}")

        for row in df_descriptions_to_process.itertuples():
            ls = row[1]

            logger.info(f"Requesting for {ls}")
            try:
                openai_response = self.get_openai_response(ls)
                logger.info(f"Response: {openai_response}")

                if len(openai_response) > 0:
                    result[row[0]] = openai_response
                    # return result

            except RateLimitError as re:
                logger.error(f"Rate limit error: {re}, exiting with obtained result")
                return result
            except Exception as e:
                logger.error(f"Unexpected error: {e}, continue with other items")
        return result

    @retry(
        wait=wait_random_exponential(min=1, max=60),
        stop=stop_after_attempt(10)
    )
    def completion_with_backoff(self, **kwargs):
        return self.openai_client.chat.completions.create(**kwargs)

    def get_openai_response(self, lines: list[str]) -> str:
        try:
            chat_completion = self.completion_with_backoff(
                messages=[
                    {
                        "role": "user",
                        "content": f"{OPENAI_REQUEST}\n{str(lines)}",
                    }
                ],
                model="gpt-3.5-turbo",
            )
            response_content = chat_completion.choices[0].message.content
            response_content = response_content.rstrip(".")
            return response_content
        except RetryError as re:
            logger.error(f"Retry error: {re}, cause: {str(re.__cause__)}")
            re.reraise()
        except Exception as e:
            logger.error(f"Error while processing {str(lines)}: {str(e)}")
            return ""
