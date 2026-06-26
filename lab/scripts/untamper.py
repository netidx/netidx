import json
inf = "/home/eric/.config/netidx/install.json"
d = json.load(open(inf))
fp = d["network"]["ca_fingerprint"]
d["network"]["ca_fingerprint"] = "PNTZY" + fp[5:]
json.dump(d, open(inf, "w"), indent=2)
print("restored ->", d["network"]["ca_fingerprint"][:11], "...")
