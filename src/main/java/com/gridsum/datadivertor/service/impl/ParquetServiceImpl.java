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

import com.gridsum.datadivertor.configuration.ConfigManager;
import com.gridsum.datadivertor.configuration.ExecutorConfig;
import com.gridsum.datadivertor.constant.Constant;
import com.gridsum.datadivertor.exception.DataDivertorException;
import com.gridsum.datadivertor.model.ParquetInfo;
import com.gridsum.datadivertor.service.ParquetService;
import com.gridsum.datadivertor.utils.PathUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroSchemaConverter;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;


public class ParquetServiceImpl implements ParquetService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetServiceImpl.class);
    private static final ExecutorConfig SCHEDULER_CONFIG = (ExecutorConfig) ConfigManager.getInstance().getBean(Constant.EXECUTOR_CONFIG);
    private static final String PARQUET_FILE_SUFFIX = ".parquet";
    private static final int PARQUET_BLOCK_SIZE = 268435456;
    private static final int PARQUET_PAGE_SIZE = 65536;
    private static final int PARQUET_DICTIONARY_PAGE_SIZE = 1048576;
    private static final boolean PARQUET_ENABLE_DICTIONARY = true;
    private static final boolean PARQUET_VALIDATING = false;
    public static final String PARQUET_AVRO_SCHEMA_PATH = "avsc/impala_query_info.avsc";

    @Override
    public ParquetInfo create(Schema avroSchema) {
        ParquetInfo parquetInfo = new ParquetInfo();

        // makesure output dir is created
        File dirFile = new File(SCHEDULER_CONFIG.getParquetTempDir());
        if (!dirFile.exists()) {
            dirFile.mkdirs();
            if (!dirFile.exists()) {
                throw new DataDivertorException(
                        String.format("dir [%s] can not found.", SCHEDULER_CONFIG.getParquetTempDir()));
            }
        }

        String fileName = UUID.randomUUID().toString() + PARQUET_FILE_SUFFIX;
        String filePath = PathUtil.makePath(SCHEDULER_CONFIG.getParquetTempDir(), fileName);

        Path output = new Path("file:" + filePath);
        MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);
        WriteSupport<IndexedRecord> writeSupport = new AvroWriteSupport(parquetSchema, avroSchema);
        Configuration configuration = new Configuration();
        try {
            parquetInfo.setFilePath(filePath);

            ParquetWriter<IndexedRecord> parquetWriter =
                    new ParquetWriter<IndexedRecord>(output, writeSupport, CompressionCodecName.SNAPPY,
                    PARQUET_BLOCK_SIZE,
                    PARQUET_PAGE_SIZE,
                    PARQUET_DICTIONARY_PAGE_SIZE,
                    PARQUET_ENABLE_DICTIONARY,
                    PARQUET_VALIDATING,
                    configuration);

            parquetInfo.setParquetWriter(parquetWriter);
        } catch (IOException e) {
            throw new DataDivertorException(
                    String.format("create parquet file [%s] failed.", parquetInfo.getFilePath()), e);
        }

        return parquetInfo;
    }

    @Override
    public void write(List<List<Object>> queryInfoList, ParquetInfo parquetInfo, Schema avroSchema) {
        if (queryInfoList.isEmpty()) {
            return;
        }

        for (List<Object> row : queryInfoList) {
            try {
                GenericRecord record = new GenericData.Record(avroSchema);
                int index = 0;
                for (Schema.Field field : avroSchema.getFields()) {
                    String fieldName = field.name();
                    Object value = row.get(index++);
                    record.put(fieldName, value);
                }
                parquetInfo.getParquetWriter().write(record);
            } catch (IOException e) {
                throw new DataDivertorException(
                        String.format("write parquet file [%s] failed.", parquetInfo.getFilePath()), e);
            }
        }
    }

    @Override
    public void close(ParquetInfo parquetInfo) {
        try {
            parquetInfo.getParquetWriter().close();

            LOGGER.info("write parquet file [{}] success.", parquetInfo.getFilePath());
        } catch (IOException e) {
            throw new DataDivertorException("close parquet writer failed.", e);
        }
    }
}
