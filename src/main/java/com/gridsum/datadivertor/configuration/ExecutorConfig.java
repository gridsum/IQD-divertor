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

package com.gridsum.datadivertor.configuration;

import com.gridsum.datadivertor.annotation.ConfigSet;


public class ExecutorConfig {
    @ConfigSet(value = "ext.datadivertor.parquet.temp.dir")
    private String parquetTempDir;
    @ConfigSet(value = "ext.datadivertor.parquet.max.query.number")
    private int parquetMaxQueryNumber;
    @ConfigSet(value = "ext.datadivertor.startup.time")
    private String startupTime;
    @ConfigSet(value = "ext.datadivertor.execution.period.minutes")
    private int executorPeriodMinutes;
    @ConfigSet(value = "ext.datadivertor.query.info.page.size")
    private int queryInfoPageSize;
    @ConfigSet(value = "ext.datadivertor.query.info.filter")
    private String queryInfoFilter;

    public String getParquetTempDir() {
        return parquetTempDir;
    }

    public int getParquetMaxQueryNumber() {
        return parquetMaxQueryNumber;
    }

    public String getStartupTime() {
        return startupTime;
    }

    public int getExecutorPeriodMinutes() {
        return executorPeriodMinutes;
    }

    public int getQueryInfoPageSize() {
        return queryInfoPageSize;
    }

    public String getQueryInfoFilter() {
        return queryInfoFilter;
    }
}
