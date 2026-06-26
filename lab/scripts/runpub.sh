#!/bin/bash
# Persistent HQ publisher over TLS. Publishes under the publisher's own
# /users subtree, where the default resolver perms grant it swlpd.
{ printf '/users/publisher.netidx.test/test/hello|string|world\n'; sleep 3600; } | netidx publisher --bind 192.168.50.0/24
