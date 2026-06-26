set -e
echo "[build+deploy] one-CA fix + single-confirm -> running role VMs"
/tmp/redeploy.sh 192.168.50.11 192.168.50.12 192.168.50.13 192.168.50.17 192.168.60.15 192.168.60.16
echo "BUILD+DEPLOY DONE"
