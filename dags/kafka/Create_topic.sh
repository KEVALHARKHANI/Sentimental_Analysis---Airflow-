/etc/confluent/docker/run &
echo -e "Waiting for Kafka to start listening on localhost"

START_TIMEOUT=600
start_timeout_exceeded=false
count=0
step=10
while netstat -lnt | awk '$4 ~ /:'"9092"'$/ {exit 1}'; do
    echo "Waiting for Kafka to be ready"
    sleep $step;
    count=$((count + step))
    if [ $count -eq $START_TIMEOUT ]; then
        start_timeout_exceeded=true
        break
    fi
done

if $start_timeout_exceeded; then
    echo "Not able to auto-create topic (waited for $START_TIMEOUT sec)"
    exit 1
fi

kafka-topics --create --if-not-exists \
    --zookeeper "zookeeper:2181" \
    --partitions 1 \
    --replication-factor 1 \
    --topic "CFA"

sleep infinity