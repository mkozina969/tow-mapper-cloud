"""Run hourly with a scheduler; defaults to a read-only dry run."""
import argparse
import os
from download_center import BUCKET, client, list_exports


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    storage = client({"endpoint": os.environ["AWS_ENDPOINT_URL_S3"], "region": os.environ["AWS_REGION"],
                      "access_key_id": os.environ["AWS_ACCESS_KEY_ID"], "secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"]})
    count = 0
    for obj in list_exports(storage, expired=True):
        if args.apply:
            storage.delete_object(Bucket=BUCKET, Key=obj["Key"])
        count += 1
    print(f"{'Deleted' if args.apply else 'Dry run: eligible objects'}: {count}")


if __name__ == "__main__":
    main()
