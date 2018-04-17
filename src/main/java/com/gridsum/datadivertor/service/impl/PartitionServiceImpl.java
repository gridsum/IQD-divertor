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

import com.gridsum.datadivertor.model.Partition;
import com.gridsum.datadivertor.service.PartitionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;


public class PartitionServiceImpl implements PartitionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionServiceImpl.class);

    @Override
    public Partition get() {
        Partition partition = new Partition();

        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTime(new Date());

        calendar.add(Calendar.DAY_OF_MONTH, -1);

        partition.setYear(calendar.get(Calendar.YEAR));
        partition.setMonth(calendar.get(Calendar.YEAR) * 100 + (calendar.get(Calendar.MONTH) + 1));
        partition.setDay(calendar.get(Calendar.YEAR) * 10000 +
                (calendar.get(Calendar.MONTH) + 1) * 100 + calendar.get(Calendar.DAY_OF_MONTH));

        return partition;
    }
}
