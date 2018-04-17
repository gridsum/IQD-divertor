## Filed Details

column | type | value | desc
--|--|--|--
query_id|string|--| Query id
query_type|string|SET,DDL,QUERY,N/A,EXPLAIN,DML,LOAD| Query type
query_state|string|FINISHED,EXCEPTION| Query state.
rows_produced|long|--| Row number of query produced.
user|string|--| User who initiates the query.
coordinator|string|--| Query coordinator server.
details_available|boolean|false,true| Is it available to get details.
database|string|--| Query database.
duration_millis|long|unit：ms| Query use time.
cluster_type|string|--| Cluster type
start_time|long|format: yyyyMMddHHmmssSSS| Query start time.
end_time|long|format: yyyyMMddHHmmssSSS| Query start time.
admission_result|string|immediately,queued,rejected,timed out| Permission to execute the query.
admission_wait|long|unit：ms| The time from submission for admission to completion of admission.
client_fetch_wait_time|long|unit：ms| The time spent waiting for the client to get the row data.
client_fetch_wait_time_percentage|double|unit：%| The percentage of client_fetch_wait_time in duration_millis
cm_cpu_milliseconds|double|unit：ms| The sum of CPU times used by all query threads
connected_user|string|--| Connected user for this session, i.e. the user which originated this session.
estimated_per_node_peak_memory|long|unit：byte| Estimation of the peak value of each query at each node.
file_formats|string|--| A list of all file formats used in a query.
hdfs_average_scan_range|double|unit：byte| Average HDFS scan range of the query.
hdfs_bytes_read|long|unit：byte| The number of bytes read from HDFS.
hdfs_bytes_read_from_cache|long|unit：byte| The number of bytes read from the HDFS cache. Only applicable to completed queries.
hdfs_bytes_read_from_cache_percentage|double|unit：%| The percentage of hdfs_bytes_read_from_cache in hdfs_bytes_read. Only applicable to completed queries.
hdfs_bytes_read_local|long|unit：byte| The number of bytes read locally from HDFS. only applicable to completed queries.
hdfs_bytes_read_local_percentage|double|unit：%| The percentage of hdfs_bytes_read_local in hdfs_bytes_read. only applicable to completed queries.
hdfs_bytes_read_remote|long|unit：byte| The number of bytes read remotely from HDFS. only applicable to completed queries.
hdfs_bytes_read_remote_percentage|double|unit：%| The percentage of hdfs_bytes_read_remote in hdfs_bytes_read. only applicable to completed queries.
hdfs_bytes_read_short_circuit|long|unit：byte| The number of bytes read from the HDFS using the short_circuit. only applicable to completed queries.
hdfs_bytes_read_short_circuit_percentage|double|unit：%| The percentage of hdfs_bytes_read_short_circuit in hdfs_bytes_read. only applicable to completed queries.
hdfs_scanner_average_bytes_read_per_second|double|unit：byte/s| Average HDFS scanner read throughput.
impala_version|string|--| Impala version
memory_spilled|long|unit：byte| Amount of memory spilled to disk
network_address|string|--| Network address
oom|string|true,false| Whether the query runs more than memory.
original_user|string|--| Original user.
planning_wait_time|long|unit：ms| Time spent in planning.
planning_wait_time_percentage|double|unit：%| The percentage of planning_wait_time in duration_millis.
pool|string|--| The resource pool that executes the query.
query_status|string|--| Query status.
session_id|string|--| Session id.
session_type|string|HIVESERVER2,BEESWAX| Session Type.
stats_missing|string|--| Whether table or column statistics are missing.
thread_cpu_time|long|unit：ms| Sum of CPU time used by all query threads.
thread_cpu_time_percentage|double|unit：%| The percentage of thread_cpu_time in thread_total_time.
thread_network_receive_wait_time|long|unit：ms| The sum of time spent by all threads waiting to receive data from the network.
thread_network_receive_wait_time_percentage|double|unit：%| The percentage of thread_network_receive_wait_time in thread_total_time.
thread_network_send_wait_time|long|unit：ms| The sum of time used to wait for all query threads to send data on the network.
thread_network_send_wait_time_percentage|double|unit：%| The percentage of thread_network_send_wait_time in thread_total_time.
thread_storage_wait_time|long|unit：ms| The sum of the time spent waiting for all query threads to store
thread_storage_wait_time_percentage|double|unit：%| The percentage of thread_storage_wait_time in thread_total_time.
thread_total_time|long|unit：ms| The sum of threads CPU, storage wait, and network waiting time used by all query threads.
bytes_streamed|long|unit：byte| Total bytes sent between the two Impala Daemons
delegated_user|string|--|--
memory_per_node_peak|double|unit：byte| The maximum amount of memory allocated by any single node
memory_per_node_peak_node|string|--| Node with the highest peak memory usage
memory_aggregate_peak|double|unit：byte| --
memory_accrual|double|unit：byte| --
ddl_type|string|--| DDL type
rows_inserted|long|--| Rows inserted
hdfs_bytes_written|long|--| Hdfs bytes written
table_list|arraylist|--| Tables involved, stored in the form of list
statement|string|--| Query sql
query_detail_info|string|--| Query details, including query plan and summery