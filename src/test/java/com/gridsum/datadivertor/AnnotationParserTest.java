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

package com.gridsum.datadivertor;

import com.gridsum.datadivertor.annotation.AnnotationParser;
import com.gridsum.datadivertor.configuration.*;
import com.gridsum.datadivertor.constant.Constant;
import junit.framework.TestCase;
import org.junit.BeforeClass;


public class AnnotationParserTest extends TestCase {

    @BeforeClass
    public void setUp() {
        System.setProperty("log.path", "./logs/");
    }

    public void testInject() {
        AdvancedProperties beans = AdvancedProperties.read(AnnotationParserTest.class.getResourceAsStream("/" + Constant.BEAN_PROPERTIES));
        AdvancedProperties configurations = AdvancedProperties.read(AnnotationParserTest.class.getResourceAsStream("/" + Constant.CONFIGURATION_PROPERTIES));
        AnnotationParser.inject(beans, configurations);

        ImpalaConfig impalaConfig = (ImpalaConfig) ConfigManager.getInstance().getBean(Constant.IMPALA_CONFIG);
        assertEquals(impalaConfig.getImpalaJDBCUrl(), "url");
        assertEquals(impalaConfig.getImpalaJDBCTable(), "table");

        KerberosConfig kerberosConfig = (KerberosConfig) ConfigManager.getInstance().getBean(Constant.KERBEROS_CONFIG);
        assertEquals(kerberosConfig.isEnable(), false);
        assertEquals(kerberosConfig.getPrinciple(), "principle");
        assertEquals(kerberosConfig.getKeytab(), "keytab");
        assertEquals(kerberosConfig.getTgtCheckMinutes(), 1);

        ClouderaManagerConfig clouderaManagerConfig = (ClouderaManagerConfig) ConfigManager.getInstance().getBean(Constant.CLOUDERA_MANAGER_CONFIG);
        assertEquals(clouderaManagerConfig.getBaseURL(), "http://host:port");
        assertEquals(clouderaManagerConfig.getUsername(), "username");
        assertEquals(clouderaManagerConfig.getPassword(), "password");
        assertEquals(clouderaManagerConfig.getApiVersion(), "v17");
        assertEquals(clouderaManagerConfig.getClusterName(), "cluster");
        assertEquals(clouderaManagerConfig.getClusterType(), "A");
        assertEquals(clouderaManagerConfig.getImpalaName(), "impala");

        ExecutorConfig executorConfig = (ExecutorConfig) ConfigManager.getInstance().getBean(Constant.EXECUTOR_CONFIG);
        assertEquals(executorConfig.getParquetTempDir(), "dir");
        assertEquals(executorConfig.getParquetMaxQueryNumber(), 5000);
        assertEquals(executorConfig.getStartupTime(), "");
        assertEquals(executorConfig.getExecutorPeriodMinutes(), 1440);
        assertEquals(executorConfig.getQueryInfoPageSize(), 1000);
        assertEquals(executorConfig.getQueryInfoFilter(), "");

        HdfsConfig hdfsConfig = (HdfsConfig) ConfigManager.getInstance().getBean(Constant.HDFS_CONFIG);
        assertEquals(hdfsConfig.getHdfsParquetTempDir(), "dir");
    }
}
