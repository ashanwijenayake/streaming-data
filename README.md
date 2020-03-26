> The repository maintains code for a streaming data pipeline built using Flink, Kafka, Elasticsearch and Kibana
The following link below is an article explaining this implementation. 

https://www.linkedin.com/pulse/streaming-data-apache-flink-ashan-wijenayake/

# Use Case  

Use Twitter Streaming API to read tweets about a given topic and perform a sentiment analysis. An identified set of KPI’s must be visualized on a dashboard.
The below mentioned are knows and how’s to quickly get-started with the above pipeline.

### Prerequisites

- A Linux operating system.
- Download and install the latest Java version. You can check if Java is already installed using the command: `java --version` in a cmd. Any version above or equal to 1.8 would be okay.

### Apache Flink
-	Download the latest Apache Flink. During this documentation, latest is 1.10.0.
https://www.apache.org/dyn/closer.lua/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.11.tgz
-	Unpack the .tgz file, using the command: `tar xzf flink-*.tgz`
-	Navigate inside the unpacked directory and open conf/flink-conf.yaml file and make the following changes.
	- rest.address 0.0.0.0
	- rest.port = 8081
- To run Flink, run the command: `./bin/start-cluster.sh` to start a single node Flink cluster.
- The front end can be accessed using the URL http://localhost:8081/
- Develop the java/ scala application with flink dependency.
- Build the jar file.
- Run the jar file using flink.
  > ` ./bin/flink run ./path/to/myjar.jar`

### Zookeeper
-	Download and unzip Zookeeper. 
https://downloads.apache.org/zookeeper/stable/apache-zookeeper-3.5.7-bin.tar.gz
-	Create zoo.cfg file in conf directory.
-	Add below configuration to zoo.cfg file.
	- tickTime = 2000
	- dataDir = /var/lib/zookeeper
	- clientPort = 2181 			
- Type `bin/zkServer.sh` in a cmd from zookeeper home directory to start 

### Apache Kafka
-	Download the 2.4.0 release and un-tar it. https://www.apache.org/dyn/closer.cgi?path=/kafka/2.4.0/kafka_2.12-2.4.0.tgz
- Create a config file for each broker 
  -	`cp config/server.properties config/server-1.properties`
-	Now edit these new files and set the following  properties
  - broker.id = 1
  - listeners = PLAINTEXT://{ip address }:9093
  -	log.dirs = /tmp/kafka-logs-1
-	Run Kafka broker by running the command `bin/kafka-server-start.sh config/server-1.properties` 

### Elasticsearch 
-	Download the elastic Search 
https://www.elastic.co/downloads/elasticsearch
-	Extract the zip file.
-	Update elasticsearch.yml file properties.
-	Network host = 0.0.0.0 and port = 9200 
-	Please follow the URL below to troubleshoot (Centos). https://www.elastic.co/guide/en/elasticsearch/reference/current/vm-max-map-count.html
https://www.elastic.co/guide/en/elasticsearch/reference/current/setting-system-settings.html#limits.conf
-	Run bin/elasticsearch file.
-	Open browser type(host:9200) i.e.- http://192.168.85.167:9200/

### Kibana
-	Download Kibana 
https://www.elastic.co/downloads/kibana 
-	Open Kibana config in an editor and point elasticsearch.hosts to your Elasticsearch instance
-	Run bin/Kibana (or bin\kibana.bat on Windows)
-	Open your browser and navigate to http://localhost:5601 

### Logstash
- Download and unzip Logstash 
https://artifacts.elastic.co/downloads/logstash/logstash-7.6.1.zip 
-	Prepare a logstash.conf config file
-	Run `bin/logstash -f logstash.conf`
-	For more information https://www.elastic.co/guide/en/logstash/current/getting-started-with-logstash.html
 
## Use case Extension
Storing the twitter data on a NoSQL database for further analytics or visualization.

### Cassandra
- Setup IntelliJ idea with python
  > Go to File  ->  Settings  ->  Plugins  ->  Select Market place and install the python plugin.
-	Please install python in your local machine and the node and set the paths to environment variables. Install the below mentioned python packages.
  - `Pip install kafka-python`
  - `Pip install Cassandra-driver`
  - `Pip install pyspark`
-	Download and install Cassandra.
https://www.howtoforge.com/tutorial/how-to-install-apache-cassandra-on-centos-7/
-	A common problem when starting Casandra is Job for cassandra.service failed because a configured resource limit was exceeded. Use the command: systemctl status cassandra.service and journalctl -xe for details about the service. The Cassandra service is not enabled on newer Linux systems, which use systemd. To verify use:  systemctl is-enabled cassandra.service. To enable the service type: systemctl enable cassandra.service.
-	Now you can start the service using:  `systemctl start cassandra.service`
-	The web dispatcher can be accessed on 127.0.0.1:9042
-	Prepare Cassandra by creating a Keyspace and Table.

```  
CREATE KEY pasanjajja WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'} AND durable_writes = true; 
```
``` 
CREATE TABLE pasanjajja.testTable (
    sentiment_score text,
    created_at text,
    tweet text,
    screen_name text,
    language text,
    location text,
    PRIMARY KEY (created_at)
); 
``` 
