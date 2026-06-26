import json
f = "/home/eric/.config/netidx/resolver.json"
d = json.load(open(f))
d["parent"]["addrs"] = []
json.dump(d, open(f, "w"), indent=2)
print("parent.addrs ->", d["parent"]["addrs"])
