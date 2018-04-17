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


import com.gridsum.datadivertor.constant.Constant;
import com.gridsum.datadivertor.exception.DataDivertorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.Strings;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ConfigCheck {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigCheck.class);
    private static final ClouderaManagerConfig CLOUDERA_MANAGER_CONFIG =
            (ClouderaManagerConfig) ConfigManager.getInstance().getBean(Constant.CLOUDERA_MANAGER_CONFIG);
    private static final ExecutorConfig EXECUTOR_CONFIG =
            (ExecutorConfig) ConfigManager.getInstance().getBean(Constant.EXECUTOR_CONFIG);

    private static final Pattern CLOUDERA_MANAGER_API_VERSION_PATTERN = Pattern.compile("^((?i)V)(1[2-9])$");
    public static final Pattern PATTERN_HOUR_MINUTE =
            Pattern.compile("^(?<hour>([0-1]{1}\\d|2[0-3])):(?<minute>([0-5]\\d))$");

    /**
     * check related configuration items.
     */
    public static void check() {
        if (!CLOUDERA_MANAGER_API_VERSION_PATTERN.matcher(CLOUDERA_MANAGER_CONFIG.getApiVersion()).matches()) {
            throw new DataDivertorException(
                    String.format("configuration item [ext.cloudera.manager.api.version] should be matches [%s]",
                            CLOUDERA_MANAGER_API_VERSION_PATTERN.toString()));
        }
        if (!Strings.isNullOrEmpty(EXECUTOR_CONFIG.getStartupTime())
                && !PATTERN_HOUR_MINUTE.matcher(EXECUTOR_CONFIG.getStartupTime()).matches()) {
            throw new DataDivertorException(
                    String.format("configuration item [ext.datadivertor.startup.time] should be matches [%s]",
                            PATTERN_HOUR_MINUTE.toString()));
        }

        LOGGER.info("check configuration item success...");
    }
}
