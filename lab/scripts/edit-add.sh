#!/bin/sh
# Fake $EDITOR: inject a new perms entry into the temp JSON non-interactively.
python3 - "$1" <<'PY'
import json, sys
f = sys.argv[1]
d = json.load(open(f))
d["/eu/secret"] = {"alice": "sw"}
json.dump(d, open(f, "w"), indent=2)
PY
