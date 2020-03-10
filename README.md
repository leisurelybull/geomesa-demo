[TOC]

# geomesa

## geomesa kafka（AIS）

​		此工程由数据产生、数据采集、数据可视化三个部分组成。

​		数据产生：数据为2015-01-01的AIS船舶数据。利用`python`脚本读取每一行数据并写入另一个文件中，模拟实时数据。

> AIS数据从https://coast.noaa.gov/官网获取。

> 下载了2015-01-01的船舶数据，下载地址：https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2015/index.html

​		数据采集：`flume`实时监控文件，当文件中有新的数据时，它将采集该数据发送给`kafka`。`geomesa kafka`订阅`kafka`的`topic`并接收其中的数据。

​		数据可视化：配置`geoserver`中的`geomesa(Kafka)`进行数据可视化。

### 重新启动

启动命令

1. 启动`zookeeper`

   ```sh
   zkServer.sh start
   ```

2. 启动`tomcat`

   ```sh
   startup.sh
   ```

3. 启动kafka

   控制台显示

   ```sh
   kafka-server-start.sh $KAFKA_HOME/config/server.properties 
   ```

   后台运行

   ```sh
   kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
   ```

4. 启动`flume`

   ```sh
   flume-ng agent --name exec-memory-kafka --conf $FLUME_HOME/conf --conf-file /home/workspace/geomesa/geomesa-spark/ais/ais_flume_kafka.conf -Dflume.root.logger=INFO,console
   ```

5. 启动应用程序

   ```sh
   java -cp geomesa-demo-kafka-1.0-SNAPSHOT.jar org.sisyphus.demo.kafka.ais.Ais
   ```

6. 执行`python`脚本，生成数据

   ```sh
   python ais.py
   ```
   
7. 打开`geoserver`进行可视化

   ```http
   http://hadoop001:8090/geoserver
   ```

### 产生数据

python模拟生成代码

使用python脚本读取`AIS_2015_01_Zone01.csv`中的数据。

```sh
vim ais.py
```

```python
#coding:utf-8
import time
# filename = 'D:\workspace\VSCode\\test\\AIS_2015_01_Zone01.csv'
# filename = '/home/workspace/geomesa/geomesa-spark/ais/AIS.csv' 
filename = '/home/workspace/geomesa/geomesa-spark/ais/AIS_2015_01_Zone01.csv'
with open(filename) as file_object:
    lines = file_object.readlines()
for line in lines:
    lineList = line.split(",")
    mmsi = lineList[0] 
    date = lineList[1]
    lat = lineList[2]
    lon = lineList[3]

    ais_temporal_log = "{mmsi}\t{date}\t{lat}\t{lon}".format(
       mmsi = mmsi,date = date,lat = lat,lon = lon
    )

    print(ais_temporal_log)
    time.sleep(1)
    # f = open('D:\workspace\VSCode\\test\\ais_temporal_log.log',"a")
    f = open('/home/workspace/geomesa/geomesa-spark/ais/ais_temporal_data.log',"a")
    f.write(ais_temporal_log+"\n")
```

将脚本上传到`linux`中，进行测试

```sh
python ais.py
```

查看有多少条输出结果

```sh
wc -l ais_data.log
```

```sh
more ais_data.log
```

Linux执行python代码

```shell
python ais.py
```

### 采集数据

第一：测试`flume`能否采集到数据，将采集到的数据输出到控制台。

测试选型如下：

> access.log ==> 控制台输出
>
> source ==> exec
>
> channel ==> memory
>
> sink ==> logger

编写配置文件`ais_flume.conf`

```sh
vim ais_flume.conf
```

```properties
exec-memory-logger.sources = exec-source
exec-memory-logger.channels = memory-channel
exec-memory-logger.sinks = logger-sink

exec-memory-logger.sources.exec-source.type = exec
exec-memory-logger.sources.exec-source.command = tail -F /home/workspace/geomesa/geomesa-spark/ais/ais_temporal_data.log
exec-memory-logger.sources.exec-source.shell = /bin/sh -c

exec-memory-logger.channels.memory-channel.type = memory

exec-memory-logger.sinks.logger-sink.type = logger

exec-memory-logger.sources.exec-source.channels = memory-channel
exec-memory-logger.sinks.logger-sink.channel = memory-channel
```

启动`flume`，观察是否有数据输出到控制台

```sh
flume-ng agent --name exec-memory-logger --conf $FLUME_HOME/conf --conf-file /home/workspace/geomesa/geomesa-spark/ais/ais_flume.conf -Dflume.root.logger=INFO,console
```

第二：使用`flume`采集生成的数据并发送到`kafka`

编写`ais_flume_kafka.conf`配置文件

使用`flume`采集生成的数据并发送到`kafka`

编写`ais_flume_kafka.conf`配置文件

选型如下：

> access.log ==> kafka
>
> source ==> exec
>
> channel ==> memory
>
> sink ==> kafka-sink

```sh
vim ais_flume_kafka.conf
```

```properties
exec-memory-kafka.sources = exec-source
exec-memory-kafka.channels = memory-channel
exec-memory-kafka.sinks = kafka-sink

exec-memory-kafka.sources.exec-source.type = exec
exec-memory-kafka.sources.exec-source.command = tail -F /home/workspace/geomesa/geomesa-spark/ais/ais_temporal_data.log
exec-memory-kafka.sources.exec-source.shell = /bin/sh -c

exec-memory-kafka.channels.memory-channel.type = memory

exec-memory-kafka.sinks.kafka-sink.type = org.apache.flume.sink.kafka.KafkaSink
exec-memory-kafka.sinks.kafka-sink.brokerList = hadoop001:9092
exec-memory-kafka.sinks.kafka-sink.topic = aistopic
exec-memory-kafka.sinks.kafka-sink.batchSize = 5
exec-memory-kafka.sinks.kafka-sink.requiredAcks = 1

exec-memory-kafka.sources.exec-source.channels = memory-channel
exec-memory-kafka.sinks.kafka-sink.channel = memory-channel
```

启动`zookeeper`(`kafka`需要)

```sh
zkServer.sh start
```

检查`zk`状态

```sh
zkServer.sh status
```

启动`kafka`

控制台显示

```sh
kafka-server-start.sh $KAFKA_HOME/config/server.properties 
```

后台执行

```sh
kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
```

> daemon 是一个守护进程，守护进行一直在后台执行

停止`kafka`

```sh
kafka-server-stop.sh $KAFKA_HOME/config/server.properties 
```

查看`topic`

```sh
kafka-topics.sh --list --zookeeper hadoop001:2181
```

创建`topic`

```sh
kafka-topics.sh --create --zookeeper hadoop001:2181 --replication-factor 1 --partitions 1 --topic streamingtopic
```

删除`topic`

```sh
kafka-topics.sh --zookeeper hadoop001:2181 --delete --topic test
```

启动flume

```sh
flume-ng agent --name exec-memory-kafka --conf $FLUME_HOME/conf --conf-file /home/workspace/geomesa/geomesa-spark/ais/ais_flume_kafka.conf -Dflume.root.logger=INFO,console
```

启动`kafka`生产者，产生数据

```sh
kafka-console-producer.sh --broker-list hadoop001:9092 --topic aistopic
```

启动`kafka`消费者，查看是否消费数据

```sh
kafka-console-consumer.sh --bootstrap-server hadoop001:9092 --topic aistopic --from-beginning --consumer-property group.id=test
```

> --from-beginning   从头开始消费
>
> --consumer-property group.id=test  指定消费组

### 数据可视化

`geoserver`中添加`geomesa(Kafka)`表中的数据进行可视化。

`spring boot`和`vue`进行前端可视化。

### 打包上传

jar包执行命令

```sh
java -cp geomesa-kafka-1.0.jar com.gis.geomesa.kafka.ais.AIS
```

```sh
java -cp jar
  com.example.geomesa.kafka.KafkaQuickStart
  -brokers brokers -zookeepers zookeepers
```

## geomesa hbase（Person）

此工程由数据的产生、采集、分析、存储和可视化。

### 重新启动

1. 启动`zookeeper`

   ```sh
   zkServer.sh start
   ```

2. 启动`hdfs`

   ```sh
   start-dfs.sh
   ```

3. 启动`hbase`

   ```sh
   start-hbase.sh
   ```

4. 启动`tomcat`

   ```sh
   startup.sh
   ```

5. 启动kafka

   ```sh
   kafka-server-start.sh $KAFKA_HOME/config/server.properties 
   ```

6. 启动`flume`

   ```sh
   flume-ng agent --name exec-memory-kafka --conf $FLUME_HOME/conf --conf-file /home/workspace/geomesa/geomesa-spark/streaming_flume_kafka.conf -Dflume.root.logger=INFO,console
   ```

7. 启动应用程序

8. 启动数据生成脚本

   ```sh
   crontab -e
   ```

### 产生数据

python模拟生成代码

使用python脚本模拟生成人的位置信息，字段为`name`、`longitude`、`latitude`、`date`。

```sh
vim point_data.py
```

```python
#coding:utf-8
import string
import random
import time

# randmon name
def name():
    return random.choice(string.ascii_uppercase)+''.join(random.choice(string.ascii_lowercase) for _ in range(4))

# random lon
def longitude():
    return "%.6f" % random.uniform(-180,180)

# random lat
def latitude():
    return "%.6f" % random.uniform(-90,90)

# localtime
def date():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

# merge all data
def spatio_temporal_data():
    # w表示会将之前的数据覆盖，a表示追加
	# f = open("D:\data\geomesa\\spatio_temporal_data.log","w+")
    # f = open("D:\data\geomesa\\spatio_temporal_data.log","a")
    f = open("/home/workspace/geomesa/geomesa-spark/person/spatio_temporal_data.log","w+")
    
    while True:
        spatio_temporal_log = "{name}\t{longitude}\t{latitude}\t{date}".format(
            name = name(),longitude=longitude(),latitude=latitude(),date = date()
        )
        
        f.write(spatio_temporal_log + "\n")
        print(spatio_temporal_log)
        time.sleep(1)

# merge all data，每一100条
def spatio_temporal_data_100(count = 100):
	# f = open("D:\data\geomesa\\spatio_temporal_data.log","w+")
    f = open("/home/workspace/geomesa/geomesa-spark/person/spatio_temporal_data.log","a")
    
    while count >= 1:
        spatio_temporal_log = "{name}\t{longitude}\t{latitude}\t{date}".format(
            name = name(),longitude=longitude(),latitude=latitude(),date = date()
        )
        
        f.write(spatio_temporal_log + "\n")
        print(spatio_temporal_log)
        count = count - 1        

if __name__ == "__main__":
    # spatio_temporal_data()
    spatio_temporal_data_100()
```

将脚本上传到`linux`中，进行测试

```sh
python point_data.py
```

查看有多少条输出结果

```sh
wc -l spatio_temporal_data.log
```

```sh
more spatio_temporal_data.log
```

Linux执行python代码

编写程序执行脚本

```sh
vim point_generator.sh
```

```sh
python /home/workspace/geomesa/geomesa-spark/point_data.py
```

给脚本添加可执行权限

```sh
chmod u+x point_generator.sh
```

执行脚本

```sh
./point_generator.sh
```

`crontab`定时调度脚本执行`python`代码

> -e：编辑某个用户的`crontab`文件内容。如果不指定用户，则表示编辑当前用户的`crontab`文件

```sh
crontab -e
```

每分钟执行一次，在文件的前面加上`#`号停止定时任务

```sh
*/1 * * * * /home/workspace/geomesa/geomesa-spark/point_generate.sh
```

`crontab`工具网址

```http
http://tool.lu/crontab
```

可以使用`tail -f`命令查看数据生成是否正常

查看后100行数据

```sh
tail -100f spatio_temporal_data.log
```

### 采集数据

测试`flume`能否采集到数据，将采集到的数据输出到控制台。

测试选型如下：

> access.log ==> 控制台输出
>
> source ==> exec
>
> channel ==> memory
>
> sink ==> logger

编写配置文件`streaming_project.conf`

```sh
vim streaming_project.conf
```

```properties
exec-memory-logger.sources = exec-source
exec-memory-logger.channels = memory-channel
exec-memory-logger.sinks = logger-sink

exec-memory-logger.sources.exec-source.type = exec
exec-memory-logger.sources.exec-source.command = tail -F /home/workspace/geomesa/geomesa-spark/person/spatio_temporal_data.log
exec-memory-logger.sources.exec-source.shell = /bin/sh -c

exec-memory-logger.channels.memory-channel.type = memory

exec-memory-logger.sinks.logger-sink.type = logger

exec-memory-logger.sources.exec-source.channels = memory-channel
exec-memory-logger.sinks.logger-sink.channel = memory-channel
```

启动flume，观察是否有数据输出到控制台

```sh
flume-ng agent --name exec-memory-logger --conf $FLUME_HOME/conf --conf-file /home/workspace/geomesa/geomesa-spark/person/streaming_project.conf -Dflume.root.logger=INFO,console
```

使用`flume`采集生成的数据并发送到`kafka`

编写`streaming_flume_kafka.conf`配置文件

选型如下：

> access.log ==> kafka
>
> source ==> exec
>
> channel ==> memory
>
> sink ==> kafka-sink

```sh
vim streaming_flume_kafka.conf
```

```properties
exec-memory-kafka.sources = exec-source
exec-memory-kafka.channels = memory-channel
exec-memory-kafka.sinks = kafka-sink

exec-memory-kafka.sources.exec-source.type = exec
exec-memory-kafka.sources.exec-source.command = tail -F /home/workspace/geomesa/geomesa-spark/person/spatio_temporal_data.log
exec-memory-kafka.sources.exec-source.shell = /bin/sh -c

exec-memory-kafka.channels.memory-channel.type = memory

exec-memory-kafka.sinks.kafka-sink.type = org.apache.flume.sink.kafka.KafkaSink
exec-memory-kafka.sinks.kafka-sink.brokerList = hadoop001:9092
exec-memory-kafka.sinks.kafka-sink.topic = streamingtopic
exec-memory-kafka.sinks.kafka-sink.batchSize = 5
exec-memory-kafka.sinks.kafka-sink.requiredAcks = 1

exec-memory-kafka.sources.exec-source.channels = memory-channel
exec-memory-kafka.sinks.kafka-sink.channel = memory-channel
```

启动`zookeeper`(`kafka`需要)

```sh
zkServer.sh start
```

检查`zk`状态

```sh
zkServer.sh status
```

启动`kafka`

控制台显示

```sh
kafka-server-start.sh $KAFKA_HOME/config/server.properties 
```

后台执行

> daemon 是一个守护进程，守护进行一直在后台执行

```sh
kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
```

停止`kafka`

```sh
kafka-server-stop.sh $KAFKA_HOME/config/server.properties 
```

查看`topic`

```sh
kafka-topics.sh --list --zookeeper hadoop001:2181
```

创建`topic`

```sh
kafka-topics.sh --create --zookeeper hadoop001:2181 --replication-factor 1 --partitions 1 --topic streamingtopic
```

删除`topic`

```sh
kafka-topics.sh --zookeeper hadoop001:2181 --delete --topic test
```

启动flume

```sh
flume-ng agent --name exec-memory-kafka --conf $FLUME_HOME/conf --conf-file /home/workspace/geomesa/geomesa-spark/person/streaming_flume_kafka.conf -Dflume.root.logger=INFO,console
```

启动`kafka`消费者，查看是否消费数据

```sh
kafka-console-consumer.sh --bootstrap-server hadoop001:9092 --topic streamingtopic --from-beginning
```

### 数据处理

`spark streaming`消费`kafka`中的数据

编写`spark streaming`程序

首先确保启动`zookeeper`、`hdfs`、`hbase`已经启动，再启动`kafka`，后启动`flume`，最后启动`sparkstreaming`应用程序。

`spark sql`离线处理。

### 数据存储

`hbase`和`hdfs`存储数据

启动`hdfs`

```sh
start-dfs.sh
```

启动`hbase`

```sh
start-hbase.sh
```

`hbase`删除数据

```sh
disable 'tablename'
drop 'tablename'
```

### 数据可视化

`geoserver`中添加`geomesa(HBase)`表中的数据进行可视化。

`spring boot`和`vue`进行前端可视化。