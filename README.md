### For windows os

Open cmd and start Zookeeper:
```
C:\kafka_2.13-3.3.1>zookeeper-server-start.bat config\zookeeper.properties
```
Open another cmd and start Kafka:
```
C:\kafka_2.13-3.3.1>kafka-server-start.bat config\server.properties
```
To run console consumer:
```
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic first_topic --group my-third-application
```
You can always use the latest version of kafka (Bi-Directional Compatibility)

To create kafka topic with name `twitter_tweets` on CLI:
```
kafka-topics --bootstrap-server 127.0.0.1:9092 --create --topic twitter_tweets --partitions 6 --replication-factor 1
```
### Producers `acks`

- acks=0 (No response is required) possibly loss data
- acks=1 (Leader response is requested, if an ask is not received, the producer may retry)
- acks=all (Leader + Replica ack requested) latency and safety, no data loss -
    - min.insync.replicas=2 at least 2 ISR (including leader) must respond they have data

### Producers `retries`
- by default 0, can be Integer.MAX_VALUE
- in kafka<0.11 to keep ordering in retries set `max.in.flight.requests.per.connection` to 1 (by default 5)

### Idempotent Producer 
 sends msg id, with this sent id kafka broker understands that this msg is already committed before.
 