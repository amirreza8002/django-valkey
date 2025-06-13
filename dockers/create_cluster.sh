#! /bin/bash

mkdir -p /nodes
touch /nodes/nodemap
if [ -z ${START_PORT} ]; then
    START_PORT=16379
fi
if [ -z ${END_PORT} ]; then
    END_PORT=16384
fi
if [ ! -z "$3" ]; then
    START_PORT=$2
    START_PORT=$3
fi
echo "STARTING: ${START_PORT}"
echo "ENDING: ${END_PORT}"

for PORT in `seq ${START_PORT} ${END_PORT}`; do
  mkdir -p /nodes/$PORT
  if [[ -e /valkey.conf ]]; then
    cp /valkey.conf /nodes/$PORT/valkey.conf
  else
    touch /nodes/$PORT/valkey.conf
  fi
  cat << EOF >> /nodes/$PORT/valkey.conf
port ${PORT}
cluster-enabled yes
daemonize yes
logfile /valkey.log
dir /nodes/$PORT
EOF

  set -x
  /usr/local/bin/valkey-server /nodes/$PORT/valkey.conf
  sleep 1
  if [ $? -ne 0 ]; then
    echo "Valkey failed to start, exiting."
    continue
  fi
  echo 127.0.0.1:$PORT >> /nodes/nodemap
done
if [ -z "${VALKEY_PASSWORD}" ]; then
    echo yes | /usr/local/bin/valkey-cli --cluster create `seq -f 127.0.0.1:%g ${START_PORT} ${END_PORT}` --cluster-replicas 1
else
    echo yes | /usr/local/bin/valkey-cli -a ${VALKEY_PASSWORD} --cluster create `seq -f 127.0.0.1:%g ${START_PORT} ${END_PORT}` --cluster-replicas 1
fi
tail -f /valkey.log