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

package com.gridsum.datadivertor.service;

import com.gridsum.datadivertor.model.ParquetInfo;
import org.apache.avro.Schema;

import java.util.List;


public interface ParquetService {

    /**
     * create a parquet writer.
     *
     * @param avroSchema the avro formatted schema.
     * @return parquet information contains the parquet writer and file path.
     */
    ParquetInfo create(Schema avroSchema);

    /**
     * write the transformed query information according to the avroSchema.
     *
     * @param queryInfoList the transformed query information.
     * @param parquetInfo the parquet information.
     * @param avroSchema the avro formatted schema.
     */
    void write(List<List<Object>> queryInfoList, ParquetInfo parquetInfo, Schema avroSchema);

    /**
     * close the parquet writer.
     *
     * @param parquetInfo parquet information contains the parquet writer and file path.
     */
    void close(ParquetInfo parquetInfo);
}
