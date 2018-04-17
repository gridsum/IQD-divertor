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

public class KerberosConfig {
    @ConfigSet(value = "ext.kerberos.enable")
    private boolean enable;
    @ConfigSet(value = "ext.kerberos.principle")
    private String principle;
    @ConfigSet(value = "ext.kerberos.keytab")
    private String keytab;
    @ConfigSet(value = "ext.kerberos.tgt.check.minutes")
    private int tgtCheckMinutes;

    public boolean isEnable() {
        return enable;
    }

    public String getPrinciple() {
        return principle;
    }

    public String getKeytab() {
        return keytab;
    }

    public int getTgtCheckMinutes() {
        return tgtCheckMinutes;
    }
}
