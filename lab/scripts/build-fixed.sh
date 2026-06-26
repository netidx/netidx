set -e
echo "[wait] devbox ssh..."
for i in $(seq 1 40); do ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=5 root@192.168.50.14 true 2>/dev/null && { echo "devbox up"; break; }; sleep 3; done
echo "[build+deploy] -> EU resolvers .60.15 .60.16"
/tmp/redeploy.sh 192.168.60.15 192.168.60.16
echo "BUILD+DEPLOY DONE"
