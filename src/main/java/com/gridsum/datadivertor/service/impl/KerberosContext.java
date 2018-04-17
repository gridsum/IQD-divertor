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
import com.gridsum.datadivertor.configuration.KerberosConfig;
import com.gridsum.datadivertor.constant.Constant;
import com.gridsum.datadivertor.exception.DataDivertorException;
import com.gridsum.datadivertor.service.LoginContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class KerberosContext implements LoginContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(KerberosContext.class);
    private static final KerberosConfig KERBEROS_CONFIG =
            (KerberosConfig) ConfigManager.getInstance().getBean(Constant.KERBEROS_CONFIG);

    private boolean enable;
    private String principle;
    private String keytab;
    private int tgtCheckMinutes;

    private UserGroupInformation user;

    private Thread thread;
    private volatile boolean cancel = false;

    private KerberosContext() {
        this.enable = KERBEROS_CONFIG.isEnable();
        this.principle = KERBEROS_CONFIG.getPrinciple();
        this.keytab = KERBEROS_CONFIG.getKeytab();
        this.tgtCheckMinutes = KERBEROS_CONFIG.getTgtCheckMinutes();

        init();
    }

    // IODH
    private static class HolderClass {
        private final static KerberosContext INSTANCE = new KerberosContext();
    }

    public static KerberosContext getInstance() {
        return HolderClass.INSTANCE;
    }

    @Override
    public UserGroupInformation get() {
        return this.user;
    }

    private void init() {
        try {
            if (this.enable) {
                LOGGER.info("KerberosContext enable");
                if (UserGroupInformation.isSecurityEnabled()) {
                    this.user = UserGroupInformation.getLoginUser();
                    if (!UserGroupInformation.isLoginTicketBased()) {
                        UserGroupInformation.loginUserFromKeytab(this.principle, this.keytab);
                        this.user = UserGroupInformation.getLoginUser();
                        LOGGER.info("init from keytab file user = {}", this.user.getUserName());

                        Runnable run = new Runnable() {
                            @Override
                            public void run() {
                                refresh();
                            }
                        };
                        this.thread = new Thread(run, "init-context-refresh-thread");
                        this.thread.setDaemon(true);
                        this.thread.start();
                    } else {
                        LOGGER.info("init from ticket cache user = {}", this.user.getUserName());
                    }
                } else {
                    LOGGER.info("hadoop security disable");
                }
            } else {
                LOGGER.info("KerberosContext disable");
            }
        } catch (IOException e) {
            throw new DataDivertorException("init kerberos error.", e);
        }
    }

    private void refresh() {
        while (!cancel) {
            try {
                Thread.sleep(this.tgtCheckMinutes * 1000 * 60);
            } catch (InterruptedException e) {
                break;
            }

            try {
                this.user.checkTGTAndReloginFromKeytab();
            } catch (IOException e) {
                LOGGER.error("checkTGTAndReloginFromKeytab error", e);
            }
        }
    }
}
