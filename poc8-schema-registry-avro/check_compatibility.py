import json
import sys

import requests

SCHEMA_REGISTRY_URL = "http://localhost:8081"
SUBJECT = "orders-value"


def main():
    if len(sys.argv) != 2:
        print("usage: python check_compatibility.py <schema_file.avsc>")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        schema_str = f.read()


    resp = requests.post(
        f"{SCHEMA_REGISTRY_URL}/compatibility/subjects/{SUBJECT}/versions?verbose=true",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        data=json.dumps({"schema": schema_str}),
    )
    resp.raise_for_status()
    result = resp.json()
    print(f"[check] {sys.argv[1]} compatible with full schema history: {result['is_compatible']}")
    if not result["is_compatible"]:
        for message in result.get("messages", []):
            print(f"  {message}")


if __name__ == "__main__":
    main()
