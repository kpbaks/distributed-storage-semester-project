#!/usr/bin/env python

import argparse
import base64
import json
import os
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file", nargs="?", help="File to upload")
    parser.add_argument("-o", "--output", help="Output file. Default: stdout")

    args = parser.parse_args()

    if not args.file:
        # read from stdin
        data = sys.stdin.buffer.read()
        filename = "stdin"
    else:
        with open(args.file, "rb") as f:
            data = f.read()
            filename = os.path.basename(args.file)

    request = {
        "filename": filename,
        "content_type": "application/octet-stream",
        "contents_b64": base64.b64encode(data).decode("utf-8"),
    }

    if args.output:
        filename = f"{args.output}_request.json"
        # write to file called file_request.json
        with open(filename, "w") as f:
            json.dump(request, f)
        print(f"Wrote {args.file}_request.json")
    else:
        print(json.dumps(request, indent=4), file=sys.stdout)
