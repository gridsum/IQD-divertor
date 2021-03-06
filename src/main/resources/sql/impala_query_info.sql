CREATE TABLE impala_query_info(
    query_id STRING,
    query_type STRING,
    query_state STRING,
    rows_produced BIGINT,
    user STRING,
    coordinator STRING,
    details_available BOOLEAN,
    `database` STRING,
    duration_millis BIGINT,
    cluster_type STRING,
    start_time BIGINT,
    end_time BIGINT,
    admission_result STRING,
    admission_wait BIGINT,
    client_fetch_wait_time BIGINT,
    client_fetch_wait_time_percentage DOUBLE,
    cm_cpu_milliseconds DOUBLE,
    connected_user STRING,
    estimated_per_node_peak_memory BIGINT,
    file_formats STRING,
    hdfs_average_scan_range DOUBLE,
    hdfs_bytes_read BIGINT,
    hdfs_bytes_read_from_cache BIGINT,
    hdfs_bytes_read_from_cache_percentage DOUBLE,
    hdfs_bytes_read_local BIGINT,
    hdfs_bytes_read_local_percentage DOUBLE,
    hdfs_bytes_read_remote BIGINT,
    hdfs_bytes_read_remote_percentage DOUBLE,
    hdfs_bytes_read_short_circuit BIGINT,
    hdfs_bytes_read_short_circuit_percentage DOUBLE,
    hdfs_scanner_average_bytes_read_per_second DOUBLE,
    impala_version STRING,
    memory_spilled BIGINT,
    network_address STRING,
    oom STRING,
    original_user STRING,
    planning_wait_time BIGINT,
    planning_wait_time_percentage DOUBLE,
    pool STRING,
    query_status STRING,
    session_id STRING,
    session_type STRING,
    stats_missing STRING,
    thread_cpu_time BIGINT,
    thread_cpu_time_percentage DOUBLE,
    thread_network_receive_wait_time BIGINT,
    thread_network_receive_wait_time_percentage DOUBLE,
    thread_network_send_wait_time BIGINT,
    thread_network_send_wait_time_percentage DOUBLE,
    thread_storage_wait_time BIGINT,
    thread_storage_wait_time_percentage DOUBLE,
    thread_total_time BIGINT,
    bytes_streamed BIGINT,
    delegated_user STRING,
    memory_per_node_peak DOUBLE,
    memory_per_node_peak_node STRING,
    memory_aggregate_peak DOUBLE,
    memory_accrual DOUBLE,
    ddl_type STRING,
    rows_inserted BIGINT,
    hdfs_bytes_written BIGINT,
    table_list ARRAY<STRING>,
    statement STRING,
    query_detail_info STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET;
