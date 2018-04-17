/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gridsum.datadivertor.service.impl;

import com.cloudera.api.ApiRootResource;
import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.model.ApiImpalaQuery;
import com.cloudera.api.model.ApiImpalaQueryDetailsResponse;
import com.cloudera.api.model.ApiImpalaQueryResponse;
import com.cloudera.api.v1.RootResourceV1;
import com.cloudera.api.v6.ImpalaQueriesResourceV6;
import com.google.common.base.Strings;
import com.gridsum.datadivertor.configuration.*;
import com.gridsum.datadivertor.configuration.ExecutorConfig;
import com.gridsum.datadivertor.constant.Constant;
import com.gridsum.datadivertor.constant.QueryAttributesConstant;
import com.gridsum.datadivertor.exception.DataDivertorException;
import com.gridsum.datadivertor.model.BatchInfo;
import com.gridsum.datadivertor.model.DateTimeSlot;
import com.gridsum.datadivertor.model.ParquetInfo;
import com.gridsum.datadivertor.model.Partition;
import com.gridsum.datadivertor.service.*;
import com.gridsum.datadivertor.utils.*;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class QueryInfoCoreProcessServiceImpl implements QueryInfoCoreProcessService {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryInfoCoreProcessServiceImpl.class);
    private static final ClouderaManagerConfig CLOUDERA_MANAGER_CONFIG =
            (ClouderaManagerConfig) ConfigManager.getInstance().getBean(Constant.CLOUDERA_MANAGER_CONFIG);
    private static final ExecutorConfig EXECUTOR_CONFIG =
            (ExecutorConfig) ConfigManager.getInstance().getBean(Constant.EXECUTOR_CONFIG);
    private static final HdfsConfig HDFS_CONFIG =
            (HdfsConfig) ConfigManager.getInstance().getBean(Constant.HDFS_CONFIG);
    private static final ImpalaConfig IMPALA_CONFIG =
            (ImpalaConfig) ConfigManager.getInstance().getBean(Constant.IMPALA_CONFIG);

    private static final Pattern COORDINATOR_PATTERN = Pattern.compile("(?<=Coordinator: )\\S*");
    private static final Pattern TABLE_LIST_PATTERN = Pattern.compile("SCAN HDFS \\[[\\s|\\S]\\]*?]");
    private static final String IMPALA_QUERY_TYPE = "QUERY";
    private static final String IMPALA_QUERY_EXCEPTION_STATE = "EXCEPTION";

    private static final String HIDDEN_FILE_PREFIX = ".";
    private static final String CRC_PARQUET_FILE_SUFFIX = ".crc";

    private static final String ALTER_TABLE_PARTITION_SQL =
            "ALTER TABLE %s ADD IF NOT EXISTS PARTITION(year=%s,month=%s,day=%s)";
    private static final String LOAD_DATA_SQL =
            "LOAD DATA INPATH '%s' INTO TABLE %s PARTITION(year=%s,month=%s,day=%s)";

    private ImpalaQueriesResourceV6 apiImpala;
    private PartitionService partitionService;
    private ParquetService parquetService;
    private HdfsService hdfsService;
    private ImpalaService impalaService;

    public QueryInfoCoreProcessServiceImpl() throws IOException {
        ApiRootResource apiRootResource = new ClouderaManagerClientBuilder()
                .withBaseURL(new URL(CLOUDERA_MANAGER_CONFIG.getBaseURL()))
                .withUsernamePassword(CLOUDERA_MANAGER_CONFIG.getUsername(), CLOUDERA_MANAGER_CONFIG.getPassword())
                .build();
        this.apiImpala = ClouderaManagerUtils.getRootResource(apiRootResource, CLOUDERA_MANAGER_CONFIG.getApiVersion())
                .getClustersResource().getServicesResource(CLOUDERA_MANAGER_CONFIG.getClusterName())
                .getImpalaQueriesResource(CLOUDERA_MANAGER_CONFIG.getImpalaName());
        this.partitionService = new PartitionServiceImpl();
        this.parquetService = new ParquetServiceImpl();
        this.hdfsService = new HdfsServiceImpl(KerberosContext.getInstance());
        this.impalaService = new ImpalaServiceImpl(KerberosContext.getInstance());
    }

    /**
     * get the datetime slot for process query information.
     * this implements is to get the datetime slot of yesterday.
     *
     * @return DateTimeSlot object to process query information.
     */
    @Override
    public DateTimeSlot get() {
        DateTimeSlot dts = new DateTimeSlot();

        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTime(DateUtil.dayStart(new Date()));
        dts.setFetchEndTime(calendar.getTime());
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        dts.setFetchStartTime(calendar.getTime());

        return dts;
    }

    /**
     * process query information.
     *
     * steps:
     * 1. fetch the query information and transfer the data to a list in current batch by page, if current
     *    datetime slot really no query information to fetch, turn to a smaller datetime slot and continue
     *    fetch the query information.
     * 2. write the transformed data to a local parquet file(consider the network issues to write remote
     *    file).
     * 3. upload the local parquet file to hdfs temporary directory, which need 777 permission if necessary.
     * 4. create the partition today if the partition does not exists.
     * 5. load the parquet file in above hdfs temporary directory to target table hdfs directory.
     * 6. loop step 1-5 until really no query information to fetch.
     *
     * @param dts DateTimeSlot object to process query information.
     * @throws DataDivertorException
     */
    @Override
    public void process(DateTimeSlot dts) throws DataDivertorException {
        Partition partition = this.partitionService.get();
        LOGGER.info("Partition: {}", partition);

        BatchInfo batchInfo = new BatchInfo();
        batchInfo.setNextBatchEndTime(dts.getFetchEndTime());

        String fetchStartTime = DateUtil.dateToString(DateUtil.cst2gmt(dts.getFetchStartTime()), DateUtil.SDF_DATETIME);
        String fetchEndTime = DateUtil.dateToString(DateUtil.cst2gmt(dts.getFetchEndTime()), DateUtil.SDF_DATETIME);
        LOGGER.info("DateTimeSlotGMT: ({}, {}]", fetchStartTime, fetchEndTime);

        Schema avroSchema = null;
        try {
            avroSchema = new Schema.Parser().parse(QueryInfoCoreProcessServiceImpl.class.getResourceAsStream(
                    "/" + ParquetServiceImpl.PARQUET_AVRO_SCHEMA_PATH));
        } catch (IOException e) {
            throw new DataDivertorException("parse avro schema failed.", e);
        }

        int rowCounter = 0;
        int totalRowCounter = 0;
        ParquetInfo parquetInfo = null;
        while (fetchEndTime.compareTo(fetchStartTime) > 0) {
            List<List<Object>> fetchedQueryInfo = fetch(fetchStartTime, fetchEndTime, batchInfo);
            if (fetchedQueryInfo.size() > 0) {
                // create a new parquet writer after current parquet file is complete.
                if (null == parquetInfo || 0 == rowCounter) {
                    parquetInfo = this.parquetService.create(avroSchema);
                }

                // write the fetched query information and close the writer when record number touched the limit.
                this.parquetService.write(fetchedQueryInfo, parquetInfo, avroSchema);
                rowCounter += fetchedQueryInfo.size();
                totalRowCounter += fetchedQueryInfo.size();
                if (rowCounter >= EXECUTOR_CONFIG.getParquetMaxQueryNumber()) {
                    this.parquetService.close(parquetInfo);
                    fileUploadAndDataLoad(parquetInfo, partition);
                    deleteCRC(parquetInfo);
                    rowCounter = 0;
                }
            } else {
                // update the datetime slot to continue fetch the query information isn't fetched.
                if (batchInfo.getPageCounter() > 0) {
                    String gmtNextBatchEndTime = DateUtil.dateToString(
                            DateUtil.cst2gmt(batchInfo.getNextBatchEndTime()), DateUtil.SDF_DATETIME);
                    if (fetchEndTime.equals(gmtNextBatchEndTime)) {
                        this.parquetService.close(parquetInfo);
                        break;
                    }

                    fetchEndTime = gmtNextBatchEndTime;
                    LOGGER.info("update DateTimeSlotGMT: ({}, {}]", fetchStartTime, fetchEndTime);
                    batchInfo.setPageCounter(0);
                    continue;
                }

                // close the writer when whole datetime slot have no query information to fetching.
                if (rowCounter > 0) {
                    this.parquetService.close(parquetInfo);
                    fileUploadAndDataLoad(parquetInfo, partition);
                    deleteCRC(parquetInfo);
                }

                break;
            }
        }

        LOGGER.info("number of fetched query info: {}", totalRowCounter);
    }

    /**
     * fetch the query information in current datetime slot by page from end time to start time.
     *
     * @param fetchStartTime start time to fetch query information.
     * @param fetchEndTime end time to fetch query information.
     * @param batchInfo current batch information.
     * @return List object contains the transformed query information data.
     */
    private List<List<Object>> fetch(String fetchStartTime, String fetchEndTime, BatchInfo batchInfo) {
        List<List<Object>> batchQueryInfoList = new ArrayList<List<Object>>();

        int pageCounter = batchInfo.getPageCounter();
        int pageSize = EXECUTOR_CONFIG.getQueryInfoPageSize();

        Date dateFetchStartTime;
        Date dateFetchEndTime;
        try {
            dateFetchStartTime = DateUtil.gmt2cst(DateUtil.stringToDate(fetchStartTime, DateUtil.SDF_DATETIME));
            dateFetchEndTime = DateUtil.gmt2cst(DateUtil.stringToDate(fetchEndTime, DateUtil.SDF_DATETIME));
        } catch (ParseException e) {
            throw new DataDivertorException("parse query end time error.", e);
        }

        ApiImpalaQueryResponse impalaResponse = apiImpala.getImpalaQueries(
                CLOUDERA_MANAGER_CONFIG.getImpalaName(), EXECUTOR_CONFIG.getQueryInfoFilter(),
                fetchStartTime, fetchEndTime, pageSize, pageCounter * pageSize);
        List<ApiImpalaQuery> queries = impalaResponse.getQueries();
        Date lateDate = null;
        for (ApiImpalaQuery query : queries) {
            // ignore the running query
            if (null == query.getEndTime()) {
                LOGGER.info("query[{}] is running.", query.getQueryId());
                continue;
            }

            // ignore the finished query but not finished between the datetime slot.
            if (query.getEndTime().getTime() <= dateFetchStartTime.getTime() ||
                    query.getEndTime().after(dateFetchEndTime)) {
                LOGGER.info("query[{}] isn't in the slot, end time: {}, fetch start time: {}, fetch end time: {}.",
                        query.getQueryId(), query.getEndTime(), dateFetchStartTime, dateFetchEndTime);
                continue;
            }
            String details;
            try {
                ApiImpalaQueryDetailsResponse apiImpalaQueryDetailsResponse =
                        apiImpala.getQueryDetails(query.getQueryId(), "text");
                details = apiImpalaQueryDetailsResponse.getDetails();
            } catch (Exception e) {
                LOGGER.warn("can not fetch details of query[{}].", query.getQueryId(), e);
                details = "";
            }

            // transform query information to list object.
            List<Object> queryInfo = impalaQueryInfoToList(query, details);
            batchQueryInfoList.add(queryInfo);
            lateDate = query.getStartTime();
        }

        // record related information for current fetched query information.
        if (batchQueryInfoList.size() > 0) {
            batchInfo.setPageCounter(++pageCounter);
            batchInfo.setNextBatchEndTime(lateDate);
            LOGGER.info("fetch [{}]rd page impala query info success.", batchInfo.getPageCounter());
        }

        return batchQueryInfoList;
    }

    /**
     * transfer the query information to a list object.
     *
     * @param query ApiImpalaQuery object.
     * @param queryDetails query details for current query.
     * @return List object contains the transformed query information data.
     */
    private List<Object> impalaQueryInfoToList(ApiImpalaQuery query, String queryDetails) {
        List<Object> result = new ArrayList<Object>();

        Map<String, String> attributes = query.getAttributes();
        String coordinator = parseCoordinator(query.getQueryType(), queryDetails);
        result.add(StringUtil.nullToEmpty(query.getQueryId()));
        result.add(StringUtil.nullToEmpty(query.getQueryType()));
        result.add(StringUtil.nullToEmpty(query.getQueryState()));
        result.add(IntegerUtil.nullToZero(query.getRowsProduced()));
        result.add(StringUtil.nullToEmpty(query.getUser()));
        result.add(StringUtil.nullToEmpty("".equals(coordinator) ? query.getCoordinator().getHostId() : coordinator));
        result.add(BooleanUtil.nullToFalse(query.getDetailsAvailable()));
        result.add(StringUtil.nullToEmpty(query.getDatabase()));
        result.add(IntegerUtil.nullToZero(query.getDurationMillis()));
        result.add(CLOUDERA_MANAGER_CONFIG.getClusterType());
        result.add(DateUtil.dateToLong(query.getStartTime(), DateUtil.SDF_DATETIME_TO_LONG));
        result.add(DateUtil.dateToLong(query.getEndTime(), DateUtil.SDF_DATETIME_TO_LONG));
        result.add(StringUtil.nullToEmpty(attributes.get(QueryAttributesConstant.ADMISSION_RESULT)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.ADMISSION_WAIT)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.CLIENT_FETCH_WAIT_TIME)));
        result.add(StringUtil.stringToDouble(attributes.get(QueryAttributesConstant.CLIENT_FETCH_WAIT_TIME_PERCENTAGE)));
        result.add(StringUtil.stringToDouble(attributes.get(QueryAttributesConstant.CM_CPU_MILLISECONDS)));
        result.add(StringUtil.nullToEmpty(attributes.get(QueryAttributesConstant.CONNECTED_USER)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.ESTIMATED_PER_NODE_PEAK_MEMORY)));
        result.add(StringUtil.nullToEmpty(attributes.get(QueryAttributesConstant.FILE_FORMATS)));
        result.add(StringUtil.stringToDouble(attributes.get(QueryAttributesConstant.HDFS_AVERAGE_SCAN_RANGE)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.HDFS_BYTES_READ)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.HDFS_BYTES_READ_FROM_CACHE)));
        result.add(StringUtil.stringToDouble(
                attributes.get(QueryAttributesConstant.HDFS_BYTES_READ_FROM_CACHE_PERCENTAGE)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.HDFS_BYTES_READ_LOCAL)));
        result.add(StringUtil.stringToDouble(attributes.get(QueryAttributesConstant.HDFS_BYTES_READ_LOCAL_PERCENTAGE)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.HDFS_BYTES_READ_REMOTE)));
        result.add(StringUtil.stringToDouble(attributes.get(QueryAttributesConstant.HDFS_BYTES_READ_REMOTE_PERCENTAGE)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.HDFS_BYTES_READ_SHORT_CIRCUIT)));
        result.add(StringUtil.stringToDouble(
                attributes.get(QueryAttributesConstant.HDFS_BYTES_READ_SHORT_CIRCUIT_PERCENTAGE)));
        result.add(StringUtil.stringToDouble(
                attributes.get(QueryAttributesConstant.HDFS_SCANNER_AVERAGE_BYTES_READ_PER_SECOND)));
        result.add(StringUtil.nullToEmpty(attributes.get(QueryAttributesConstant.IMPALA_VERSION)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.MEMORY_SPILLED)));
        result.add(StringUtil.nullToEmpty(attributes.get(QueryAttributesConstant.NETWORK_ADDRESS)));
        result.add(StringUtil.nullToEmpty(attributes.get(QueryAttributesConstant.OOM)));
        result.add(StringUtil.nullToEmpty(attributes.get(QueryAttributesConstant.ORIGINAL_USER)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.PLANNING_WAIT_TIME)));
        result.add(StringUtil.stringToDouble(attributes.get(QueryAttributesConstant.PLANNING_WAIT_TIME_PERCENTAGE)));
        result.add(StringUtil.nullToEmpty(attributes.get(QueryAttributesConstant.POOL)));
        result.add(StringUtil.nullToEmpty(attributes.get(QueryAttributesConstant.QUERY_STATUS)));
        result.add(StringUtil.nullToEmpty(attributes.get(QueryAttributesConstant.SESSION_ID)));
        result.add(StringUtil.nullToEmpty(attributes.get(QueryAttributesConstant.SESSION_TYPE)));
        result.add(StringUtil.nullToEmpty(attributes.get(QueryAttributesConstant.STATS_MISSING)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.THREAD_CPU_TIME)));
        result.add(StringUtil.stringToDouble(attributes.get(QueryAttributesConstant.THREAD_CPU_TIME_PERCENTAGE)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.THREAD_NETWORK_RECEIVE_WAIT_TIME)));
        result.add(StringUtil.stringToDouble(
                attributes.get(QueryAttributesConstant.THREAD_NETWORK_RECEIVE_WAIT_TIME_PERCENTAGE)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.THREAD_NETWORK_SEND_WAIT_TIME)));
        result.add(StringUtil.stringToDouble(
                attributes.get(QueryAttributesConstant.THREAD_NETWORK_SEND_WAIT_TIME_PERCENTAGE)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.THREAD_STORAGE_WAIT_TIME)));
        result.add(StringUtil.stringToDouble(
                attributes.get(QueryAttributesConstant.THREAD_STORAGE_WAIT_TIME_PERCENTAGE)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.THREAD_TOTAL_TIME)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.BYTES_STREAMED)));
        result.add(StringUtil.nullToEmpty(attributes.get(QueryAttributesConstant.DELEGATED_USER)));
        result.add(StringUtil.stringToDouble(attributes.get(QueryAttributesConstant.MEMORY_PER_NODE_PEAK)));
        result.add(StringUtil.nullToEmpty(attributes.get(QueryAttributesConstant.MEMORY_PER_NODE_PEAK_NODE)));
        result.add(StringUtil.stringToDouble(attributes.get(QueryAttributesConstant.MEMORY_AGGREGATE_PEAK)));
        result.add(StringUtil.stringToDouble(attributes.get(QueryAttributesConstant.MEMORY_ACCRUAL)));
        result.add(StringUtil.nullToEmpty(attributes.get(QueryAttributesConstant.DDL_TYPE)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.ROWS_INSERTED)));
        result.add(StringUtil.stringToLong(attributes.get(QueryAttributesConstant.HDFS_BYTES_WRITTEN)));
        result.add(parseJoinedTables(query.getQueryType(), query.getQueryState(), queryDetails));
        result.add(StringUtil.nullToEmpty(query.getStatement()));
        result.add(StringUtil.nullToEmpty(queryDetails));

        return result;
    }

    /**
     * parse coordinator from query details with query type QUERY.
     *
     * @param queryType query type.
     * @param queryDetails query details.
     * @return String object contains parsed coordinator.
     */
    private String parseCoordinator(String queryType, String queryDetails) {
        String coordinator = "";
        if ("QUERY".equals(queryType)) {
            Matcher matcher = COORDINATOR_PATTERN.matcher(queryDetails);
            if (matcher.find()) {
                coordinator = matcher.group(0);
            }
        }

        return coordinator;
    }

    /**
     * parse joined tables from query details.
     *
     * @param queryType query type.
     * @param queryState query state.
     * @param queryDetails query details.
     * @return Set object contains parsed joined tables.
     */
    private Set<String> parseJoinedTables(String queryType, String queryState, String queryDetails) {
        Set<String> result = new HashSet<String>();
        if (Strings.isNullOrEmpty(queryDetails)) {
            return result;
        }
        if (IMPALA_QUERY_TYPE.equals(queryType) && !IMPALA_QUERY_EXCEPTION_STATE.equals(queryState)) {
            Matcher matcher = TABLE_LIST_PATTERN.matcher(queryDetails);
            while (matcher.find()) {
                result.add(matcher.group());
            }
        }

        return result;
    }

    /**
     * upload local parquet file to hdfs temporary directory and load the data to target table directory.
     *
     * @param parquetInfo parquet information.
     * @param partition current partition.
     */
    private void fileUploadAndDataLoad(ParquetInfo parquetInfo, Partition partition) {
        this.hdfsService.fromLocalToHDFS(parquetInfo.getFilePath(), HDFS_CONFIG.getHdfsParquetTempDir());

        LOGGER.info("upload parquet file [{}] success.", parquetInfo.getFilePath());

        String parquetName = new File(parquetInfo.getFilePath()).getName();
        String hdfsParquetPath = PathUtil.makePath(HDFS_CONFIG.getHdfsParquetTempDir(), parquetName);
        this.impalaService.execute(String.format(ALTER_TABLE_PARTITION_SQL,
                IMPALA_CONFIG.getImpalaJDBCTable(),
                partition.getYear(),
                partition.getMonth(),
                partition.getDay()));

        LOGGER.info("check partition success.");

        this.impalaService.execute(String.format(LOAD_DATA_SQL,
                hdfsParquetPath,
                IMPALA_CONFIG.getImpalaJDBCTable(),
                partition.getYear(),
                partition.getMonth(),
                partition.getDay()));

        LOGGER.info("load parquet file [{}] success.", parquetInfo.getFilePath());
    }

    /**
     * delete crc file generated in create local parquet file.
     *
     * @param parquetInfo parquet information.
     */
    private void deleteCRC(ParquetInfo parquetInfo) {
        File file = new File(PathUtil.makePath(HIDDEN_FILE_PREFIX, parquetInfo.getFilePath(), CRC_PARQUET_FILE_SUFFIX));
        if (file.isFile() && file.exists()) {
            file.delete();
        }
    }
}
