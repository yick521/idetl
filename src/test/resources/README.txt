工程的入口在
com.zhugeio.etl.id.IdApp


主Service
com.zhugeio.etl.id.service.MainService


create topic

bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic statis --partitions=1 --replication-factor 1
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic zg_zgid --partitions=1 --replication-factor 1
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic zg_user --partitions=1 --replication-factor 1
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic zg_pl --partitions=1 --replication-factor 1
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic zg_st --partitions=1 --replication-factor 1
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic zg_se --partitions=1 --replication-factor 1
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic zg_et --partitions=1 --replication-factor 1


create table
