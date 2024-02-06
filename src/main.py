import argparse
from logger import get_logger
from service import Params, ProcessingService

logger = get_logger(__name__)

if __name__ == '__main__':
    logger.info("Starting")

    parser = argparse.ArgumentParser()
    parser.add_argument("connection_string", help="Database connection string")
    parser.add_argument("openai_api_key", help="OpenAI API Key")
    parser.add_argument("snapshot_id", help="Snapshot Id")

    args = parser.parse_args()

    params = Params(args.connection_string, args.openai_api_key, args.snapshot_id)
    logger.info(f"Params: {params}")

    service = ProcessingService(params)
    service.execute()
