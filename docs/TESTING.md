# Local Testing Guide

## Setup

1. Install the tap:
```bash
pip install -e .
export PATH="$PATH:/home/ubuntu/.local/bin"
```

2. Create `testconfig.json` (gitignored):
```json
{
  "tilroy_api_key": "YOUR_KEY",
  "x_api_key": "YOUR_KEY",
  "api_url": "https://api.tilroy.com"
}
```

## Discovery

Check all streams are registered:
```bash
tap-tilroy --config testconfig.json --discover
```

Parse specific stream:
```bash
tap-tilroy --config testconfig.json --discover 2>&1 | python3 -c "
import json, sys
data = sys.stdin.read()
start = data.find('{')
catalog = json.loads(data[start:])
for s in catalog['streams']:
    print(f\"{s['tap_stream_id']}: {s.get('replication_key', 'full-table')}\")
"
```

## Create Test Catalog (single stream)

```bash
tap-tilroy --config testconfig.json --discover 2>&1 | python3 -c "
import json, sys
data = sys.stdin.read()
start = data.find('{')
catalog = json.loads(data[start:])

STREAM_NAME = 'transfers'  # Change this

for stream in catalog['streams']:
    selected = stream['tap_stream_id'] == STREAM_NAME
    stream['metadata'] = [
        {'breadcrumb': [], 'metadata': {'selected': selected}}
    ]

print(json.dumps(catalog, indent=2))
" > test_catalog_single.json
```

## Create State File

```json
{
  "bookmarks": {
    "stream_name": {
      "replication_key": "dateExported",
      "replication_key_value": "2025-12-15T00:00:00Z"
    }
  }
}
```

## Run Sync

Full sync (no state):
```bash
tap-tilroy --config testconfig.json --catalog test_catalog_single.json
```

Incremental sync (with state):
```bash
tap-tilroy --config testconfig.json --catalog test_catalog_single.json --state test_state.json
```

## Direct API Testing

```bash
curl -s \
  -H "Tilroy-Api-Key: YOUR_KEY" \
  -H "x-api-key: YOUR_KEY" \
  "https://api.tilroy.com/ENDPOINT?param=value"
```

## Common Issues

### "command not found: tap-tilroy"
```bash
export PATH="$PATH:/home/ubuntu/.local/bin"
```

### Schema errors
The SDK is strict about schema. If you see type mismatches:
1. Check the schema definition in the stream
2. Add type coercion in `post_process()`
3. Use `CustomType({"type": ["string", "number", "null"]})` for flexible fields

### 400 Bad Request
- Check required parameters (dateFrom, etc.)
- Check authentication headers
- Try direct curl to see actual error message

### Empty results
- Verify the tenant has data for that stream
- Try without state file (full sync)
- Check date range isn't in the future
