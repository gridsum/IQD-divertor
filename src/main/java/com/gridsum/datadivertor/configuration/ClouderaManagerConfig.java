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


public class ClouderaManagerConfig {
    @ConfigSet(value = "ext.cloudera.manager.base.url")
    private String baseURL;
    @ConfigSet(value = "ext.cloudera.manager.username")
    private String username;
    @ConfigSet(value = "ext.cloudera.manager.password")
    private String password;
    @ConfigSet(value = "ext.cloudera.manager.api.version")
    private String apiVersion;
    @ConfigSet(value = "ext.cloudera.manager.cluster.name")
    private String clusterName;
    @ConfigSet(value = "ext.cloudera.manager.cluster.type")
    private String clusterType;
    @ConfigSet(value = "ext.cloudera.manager.service.impala.name")
    private String impalaName;

    public String getBaseURL() {
        return baseURL;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getApiVersion() {
        return apiVersion;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getClusterType() {
        return clusterType;
    }

    public String getImpalaName() {
        return impalaName;
    }
}
