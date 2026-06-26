#!/bin/sh
# wan — shape inter-segment (WAN) transit on the 3-leg router, never the
# router's own traffic, so a 100% partition can't lock you out of SSH.
#
#   enp1s0 = HQ  leg (192.168.50.0/24, head office, serves /)
#   enp7s0 = EU  leg (192.168.60.0/24, satellite, serves /eu)
#   enp8s0 = AP  leg (192.168.70.0/24, satellite, serves /ap)
#
# Transit is matched by SOURCE subnet on egress and steered into a high prio
# band the default priomap never uses; the router's own src (.x.2) is never
# matched, so it is never shaped. Each cross-segment packet crosses exactly
# one shaped leg per direction, so a per-leg delay D yields ~2*D RTT.
#
# Forms:
#   wan clear|show
#   wan delay D [J] | loss P | link D J LOSS | raw "<netem>"   (ALL WAN links)
#   wan seg <hq|eu|ap> <delay D [J]|loss P|link D J LOSS|raw "<netem>">
#       shape ONLY that segment's link (both directions); the other two
#       segments keep talking cleanly. e.g. `wan seg ap loss 100%` isolates
#       the AP satellite while HQ<->EU stays up.
HQ_DEV=enp1s0; HQ_NET=192.168.50.0/24
EU_DEV=enp7s0; EU_NET=192.168.60.0/24
AP_DEV=enp8s0; AP_NET=192.168.70.0/24
ALL_DEVS="$HQ_DEV $EU_DEV $AP_DEV"

reset_leg() { tc qdisc del dev "$1" root 2>/dev/null; }
clear_all() { for d in $ALL_DEVS; do reset_leg "$d"; done; }

# (re)create the prio+netem qdisc on a leg and steer the given src subnets
# into the shaped band. $1=dev $2=netem-spec $3.. = src subnets
shape_leg() {
  dev=$1; spec=$2; shift 2
  tc qdisc del dev "$dev" root 2>/dev/null
  tc qdisc add dev "$dev" root handle 1: prio bands 4
  tc qdisc add dev "$dev" parent 1:4 handle 40: netem $spec
  for s in "$@"; do
    tc filter add dev "$dev" parent 1:0 protocol ip u32 match ip src "$s" flowid 1:4
  done
}

apply_all() {   # uniform: every leg shapes transit from the other two subnets
  clear_all
  shape_leg "$HQ_DEV" "$1" "$EU_NET" "$AP_NET"
  shape_leg "$EU_DEV" "$1" "$HQ_NET" "$AP_NET"
  shape_leg "$AP_DEV" "$1" "$HQ_NET" "$EU_NET"
}

apply_seg() {   # $1=spec $2=this-leg-dev $3=this-net  (the other two are inferred)
  spec=$1; sdev=$2; snet=$3; clear_all
  o1=""; o2=""; d1=""; d2=""
  for pair in "$HQ_DEV:$HQ_NET" "$EU_DEV:$EU_NET" "$AP_DEV:$AP_NET"; do
    dev=${pair%%:*}; net=${pair#*:}
    [ "$dev" = "$sdev" ] && continue
    if [ -z "$d1" ]; then d1=$dev; o1=$net; else d2=$dev; o2=$net; fi
  done
  shape_leg "$sdev" "$spec" "$o1" "$o2"   # everything -> segment
  shape_leg "$d1"   "$spec" "$snet"       # segment -> other1
  shape_leg "$d2"   "$spec" "$snet"       # segment -> other2
}

spec_from() {   # builds a netem spec from "delay/loss/link/raw ..." starting at $1
  case "$1" in
    delay) if [ -n "$3" ]; then echo "delay $2 $3 distribution normal"; else echo "delay $2"; fi;;
    loss)  echo "loss $2";;
    link)  echo "delay $2 $3 distribution normal loss $4";;
    raw)   echo "$2";;
    *)     return 1;;
  esac
}

case "$1" in
  clear) clear_all; echo "wan: cleared (clean links)";;
  show)  for d in $ALL_DEVS; do echo "## $d"; tc -s qdisc show dev "$d"; tc filter show dev "$d" 2>/dev/null; done;;
  seg)
    seg=$2; shift 2
    spec=$(spec_from "$@") || { echo "bad spec"; exit 1; }
    case "$seg" in
      hq) apply_seg "$spec" "$HQ_DEV" "$HQ_NET";;
      eu) apply_seg "$spec" "$EU_DEV" "$EU_NET";;
      ap) apply_seg "$spec" "$AP_DEV" "$AP_NET";;
      *)  echo "wan seg: pick hq|eu|ap"; exit 1;;
    esac
    echo "wan: '$spec' on the $seg link only (both directions)";;
  delay|loss|link|raw)
    spec=$(spec_from "$@") || { echo "bad spec"; exit 1; }
    apply_all "$spec"; echo "wan: '$spec' on all WAN transit (each leg)";;
  *) echo 'usage: wan {clear | show | <cond> | seg <hq|eu|ap> <cond>}   cond = delay D [J] | loss P | link D J LOSS | raw "<netem>"';;
esac
