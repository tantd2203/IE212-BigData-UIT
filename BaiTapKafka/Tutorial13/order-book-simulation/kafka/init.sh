#! /bin/zsh

# Note that I added kafka commands to the PATH. 
# You may need to adjust your .bashrc or .zshrc file and modify file paths like 
# /usr/local/etc/kafka/server.properties to run this file.

# Step 1, if Kafka server is not running, start it. 

# kafka-server-start /usr/local/etc/kafka/server.properties

# Another way is to use Homebrew on Mac with `brew services start kafka` or `brew services stop kafka`.

# If topics exist, delete them first
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic incoming-order
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic fulfill-notification
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic current-price

# Step 2, create 3 topics whose names are
# 1, incoming-order
# 2, fulfill-notification
# 3, current-price

kafka-topics.sh --create --topic incoming-order --bootstrap-server localhost:9092
kafka-topics.sh --create --topic fulfill-notification --bootstrap-server localhost:9092
kafka-topics.sh --create --topic current-price --bootstrap-server localhost:9092

# To check existing topics:
# kafka-topics --list --bootstrap-server localhost:9092

# To delete a topic:
# kafka-topics --bootstrap-server localhost:9092 --delete --topic <topic-name>
# note that you need to enable topic deletion in `server.properties` file by adding the following line without `#`
# delete.topic.enable=true 

# Step 3, check the status of Kafka topics.

kafka-topics.sh --list --bootstrap-server localhost:9092

