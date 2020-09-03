

# **SilverDB**

SilverDB is an open source distributed time series database. This includes APIs for storing and querying data, processing and more. The master branch on this repo now represents the original SilverDB.

## **Features**

1、Distributed read and write for time-series data and storage based on boltdb 

2、Compression for data types,such as timestamp、float64、Integer、String、Boolean

3、Support metircs and tags retrieval within time range 

4、Support the storage of tags index data 

5、Support wal log and memory caching 

6、Support silverDB node,databasea and table metadata registration based on etcd 

## **State of the project**  

The SilverDB that is on the master branch is currently in the beta stage. This means that it is still **NOT** recommended for production usage.。dev-1.0 branch is improving  tests、features for storage、write、read.  

### **Notice** 

SilverDB is a distributed time-series database project that just begin to design and develop , and is still in the initial inclubation stage.

You can develop and debug some features of SilverDB  in the local environment,such as storage,read and write。

## **Building From Source**

### Prerequisites for building SilverDB: 

This project requries Go 1.14 and Go module support 

Linux、Mac OS x、Windows 

git 

protobuf  3.3.0+

etcd 3.4.9

### **Configure SilverDB** 

config.yaml 

```
nodeName: "node1"     
nodeId: 1
nodeAddr:
  httpAddr: "127.0.0.1:12345"     #httpd service 
  cluAddr: "127.0.0.1:7947"       
  tcpAddr: "127.0.0.1:12346"      #database serivce 
dataDir: ["/Volumes/info/dev/silver/testdata1/","/Volumes/info/dev/silver/testdata2/"]
indexDir: ["/Volumes/info/dev/silver/index1/"]
wal:
  walDir: "/Volumes/info/dev/silver/wal1/"
  ttl: 1                          #wal flush write data ttl 
  nodeNums: 100                   
  listNums: 500                   
  size: 100                       #wal buffer size 
flush:
  count: 5000                     # buffer flush count 
  ttl: 3
heatbeat:
  timeout: 5
  tick: 5
metastore:
  metaAddr: ["127.0.0.1:2379"]     #ectd server 
  metaPrefix: "/silver/metaData/"  # metadata key 
  nodePrefix: "/sliver/node/"      # siverDB node key  
  timeout: 10            
  heartbeat: 5
discovery:
  timeout: 10
compressed: true                       
compressCount: 50000               # data compress size 
indexed: true
```



git  clone https://github.com/rolls-tech/silverDB.git

cd silverDB 

go build node1.go  

## **Documention** 

The documentation of  SilverDB is located on the website: https://zhuanlan.zhihu.com/c_1256552728916017152 
