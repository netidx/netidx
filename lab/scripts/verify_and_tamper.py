import json
rf = "/home/eric/.config/netidx/resolver.json"
print("parent.addrs now:", json.load(open(rf))["parent"]["addrs"])
# Tamper the pinned CA fingerprint with a valid-base32-but-wrong value
# (first group PNTZY -> ABCDE) to trigger the mismatch refusal.
inf = "/home/eric/.config/netidx/install.json"
d = json.load(open(inf))
fp = d["network"]["ca_fingerprint"]
d["network"]["ca_fingerprint"] = "ABCDE" + fp[5:]
json.dump(d, open(inf, "w"), indent=2)
print("tampered fingerprint ->", d["network"]["ca_fingerprint"][:11], "...")
