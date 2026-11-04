"""Run hourly with a scheduler; defaults to a read-only dry run."""
import argparse
import os
from download_center import BUCKET, client, list_exports

STAGING_BRANCH = "br-delicate-truth-b9ptbns0"
STAGING_ENDPOINT = f"https://{STAGING_BRANCH}.storage.c-7.eu-central-1.aws.neon.tech"
STAGING_REGION = "eu-central-1"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--staging-only", action="store_true")
    args = parser.parse_args()
    if args.staging_only:
        if (os.environ.get("AWS_ENDPOINT_URL_S3") != STAGING_ENDPOINT
                or os.environ.get("AWS_REGION") != STAGING_REGION):
            parser.error("Cleanup refused: endpoint and region must match the pinned Neon staging branch.")
        if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
            parser.error("Cleanup refused: staging storage credentials are required.")
        print(f"Validated staging branch {STAGING_BRANCH}, bucket {BUCKET}", flush=True)
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
