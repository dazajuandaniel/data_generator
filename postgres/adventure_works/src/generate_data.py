import argparse
from logger import get_logger

from crud import loggy, GenerateData

loggy = get_logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", type=str,
                        default='create', choices=['create', 'edit', 'delete'])
    parser.add_argument("--table", type=str, default=None)
    parser.add_argument("--num_records", type=int, default=1)
    args, _ = parser.parse_known_args()

    loggy.info("Received arguments {}".format(args))

    generate_data = GenerateData(
        action=args.action, table=args.table, num_records=args.num_records)
    generate_data.create_data()
