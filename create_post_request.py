#!/usr/bin/env python

import json
import os
import argparse
import sys
import base64

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('file', help='File to upload')
    parser.add_argument('-o', '--output', help='Output file (default: <file>_request.json)')

    args = parser.parse_args()

    with open(args.file, 'rb') as f:
        data = f.read()
        request = {
            'filename': os.path.basename(args.file),
        	"content_type": "application/octet-stream",
            'contents_b64': base64.b64encode(data).decode('utf-8'),
        }
    
    if args.output:
        filename = f"{args.output}_request.json"
        # write to file called file_request.json
        with open(filename, 'w') as f:
            json.dump(request, f)
        print(f'Wrote {args.file}_request.json')
    else:
        print(json.dumps(request, indent=4), file=sys.stdout)
    