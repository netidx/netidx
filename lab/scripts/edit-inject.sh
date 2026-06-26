#!/bin/sh
# Fake $EDITOR: merge /tmp/inject.json into the temp perms file.
python3 - "$1" <<'PY'
import json, sys
f = sys.argv[1]
d = json.load(open(f))
d.update(json.load(open("/tmp/inject.json")))
json.dump(d, open(f, "w"), indent=2)
PY
