/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gridsum.datadivertor;

import com.gridsum.datadivertor.annotation.AnnotationParser;
import com.gridsum.datadivertor.configuration.AdvancedProperties;
import com.gridsum.datadivertor.configuration.ConfigCheck;
import com.gridsum.datadivertor.configuration.ConfigManager;
import com.gridsum.datadivertor.configuration.ExecutorConfig;
import com.gridsum.datadivertor.constant.Constant;
import com.gridsum.datadivertor.exception.DataDivertorException;
import com.gridsum.datadivertor.task.QueryInfoProcessTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.Strings;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;


public class ScheduledExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledExecutor.class);

    private ScheduledExecutorService schedulerService;
    private ExecutorConfig executorConfig;

    /**
     * Create a scheduled executor.
     *
     * @param corePoolSize the size of thread pool.
     * @param configPath the configuration path(the conf directory in project construction).
     */
    ScheduledExecutor(int corePoolSize, String configPath) {
        this.schedulerService = Executors.newScheduledThreadPool(corePoolSize);

        // read beans
        AdvancedProperties beans = AdvancedProperties.read(
                ScheduledExecutor.class.getResourceAsStream("/" + Constant.BEAN_PROPERTIES));
        // read configuration items
        AdvancedProperties configurations = AdvancedProperties.read(configPath + Constant.CONFIGURATION_PROPERTIES);
        // inject configuration items to the beans
        AnnotationParser.inject(beans, configurations);

        this.executorConfig = (ExecutorConfig) ConfigManager.getInstance().getBean(Constant.EXECUTOR_CONFIG);
    }

    public ScheduledExecutorService getSchedulerService() {
        return this.schedulerService;
    }

    public ExecutorConfig getExecutorConfig() {
        return executorConfig;
    }

    /**
     * main function to schedule query information process task.
     *
     * @param args
     */
    public static void main(String[] args) {
        String configPath = System.getProperty("config.path");
        if (null == configPath) {
            System.out.println("jvm option [config.path] is unset");
            System.exit(1);
        }

        ScheduledExecutorService schedulerService = null;
        try {
            ScheduledExecutor executor = new ScheduledExecutor(Constant.TREAD_NUMBER_OF_SCHEDULED_EXECUTOR, configPath);

            schedulerService = executor.getSchedulerService();

            ExecutorConfig executorConfig = executor.getExecutorConfig();

            ConfigCheck.check();

            QueryInfoProcessTask task = new QueryInfoProcessTask();
            long firstTimeStartupDelayMinutes = calculateDelayMinutes(executorConfig.getStartupTime());
            LOGGER.info("first time startup delay minutes: {}", firstTimeStartupDelayMinutes);
            schedulerService.scheduleAtFixedRate(task, firstTimeStartupDelayMinutes,
                    executorConfig.getExecutorPeriodMinutes(), TimeUnit.MINUTES);

        } catch (IOException e) {
            if (null != schedulerService) {
                schedulerService.shutdown();
            }
            LOGGER.error("executor exit", e);
        }
    }

    /**
     * calculate first time startup delay minutes for query information process task.
     * if startupTime is unset, returns 0;
     * if startupTime is set and startupTime < now，returns the minute difference between now
     * and startupTime tomorrow, otherwise returns the minute difference between now and
     * startupTime today.
     *
     * @param startupTime designated time to startup query information process task.
     * @return minute difference.
     */
    private static long calculateDelayMinutes(String startupTime) {
        if (Strings.isNullOrEmpty(startupTime)) {
            return 0;
        }

        Matcher matcher = ConfigCheck.PATTERN_HOUR_MINUTE.matcher(startupTime);
        if (!matcher.matches()) {
            throw new DataDivertorException(
                    "formation of configuration item [ext.datadivertor.startup.time] does not match.");
        }

        GregorianCalendar nowCalendar = new GregorianCalendar();
        nowCalendar.setTime(new Date());

        GregorianCalendar timerCalendar = new GregorianCalendar();
        timerCalendar.setTime(nowCalendar.getTime());
        timerCalendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(matcher.group("hour")));
        timerCalendar.set(Calendar.MINUTE, Integer.parseInt(matcher.group("minute")));
        timerCalendar.set(Calendar.SECOND, 0);
        timerCalendar.set(Calendar.MILLISECOND, 0);

        if (timerCalendar.getTime().before(nowCalendar.getTime())) {
            timerCalendar.add(Calendar.DAY_OF_MONTH, 1);
        }

        return (timerCalendar.getTimeInMillis() - nowCalendar.getTimeInMillis()) / 1000 / 60;
    }
}
