# 1. Introduction
**IQD-divertor** is a crawler that crawls the Impala query information from Cloudera 
Manager. It will store the scrawled information to a prepared Impala table, then 
provide to relevant analyst for data analysis. 


# 2. Installation

## 2.1. Dependencies
 - JDK 7
 - Maven( maven>=3.0.0 )
 - Cloudera Manager( cdh>=5.7.2 )
 - Cloudera Impala( >=impala-2.5.0+cdh5.7.2 )


## 2.2. Install
 - Clone source code and export environment variable. 
 >
    git clone https://github.com/gridsum/IQD-divertor.git  
    cd IQD-divertor  
    echo "export IQD_DIVERTOR_HOME=\`pwd\`" >> ~/.bashrc  

 - Create the Impala table in your target database with the sql in the [file](./src/main/resources/sql/impala_query_info.sql). 


## 2.3. Configure & Package
 - Edit the IQD-divertor [configuration](./conf/configuration.properties), recommended to modify the configuration items according to the annotations.

 - Import the Hadoop configurations **core-site.xml** and **hdfs-site.xml** to [project resources dir](./src/main/resources/).

 - Package the project with Maven.
 > 
    mvn clean package
    

## 2.4. Start/Stop daemon
 - Start daemon: 
 >
    ./bin/deamon.sh start

 - Stop daemon: 
 >
    ./bin/deamon.sh stop
    

# 3. Tutorials & Documentation

## 3.1. Principle
The principle is mainly as follows:
 - Firstly, crawl the Impala query information with Cloudera Manager API.
 - Secondly, extract the related fields and write to parquet file.
 - Thirdly, upload the Parquet file to HDFS.
 - Finally, load the Parquet formated data to target Impala table.


## 3.2. Fields in impala table
See [fields.md](./fields.md) for details about fields.


# 4. Communication
  impala-toolbox-help@gridsum.com


# 5. License
IQD-divertor is [licensed under the Apache License 2.0.](./LICENSE)


