For windows os


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