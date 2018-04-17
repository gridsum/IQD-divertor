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

package com.gridsum.datadivertor.task;

import com.gridsum.datadivertor.model.DateTimeSlot;
import com.gridsum.datadivertor.service.QueryInfoCoreProcessService;
import com.gridsum.datadivertor.service.impl.QueryInfoCoreProcessServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class QueryInfoProcessTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryInfoProcessTask.class);

    private QueryInfoCoreProcessService crawlerService;

    public QueryInfoProcessTask() throws IOException {
        this.crawlerService = new QueryInfoCoreProcessServiceImpl();
    }

    /**
     * the implement of query information process task.
     */
    @Override
    public void run() {
        long startTime = System.currentTimeMillis();

        DateTimeSlot dtsYesterday = this.crawlerService.get();
        LOGGER.info("DateTimeSlot: {}", dtsYesterday);

        this.crawlerService.process(dtsYesterday);

        long endTime = System.currentTimeMillis();
        LOGGER.info("Spent time: {}ms", (endTime - startTime));
    }
}
