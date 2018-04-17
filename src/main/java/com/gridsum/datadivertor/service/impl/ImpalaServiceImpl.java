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
import com.gridsum.datadivertor.configuration.ImpalaConfig;
import com.gridsum.datadivertor.constant.Constant;
import com.gridsum.datadivertor.exception.DataDivertorException;
import com.gridsum.datadivertor.service.ImpalaService;
import com.gridsum.datadivertor.service.LoginContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class ImpalaServiceImpl implements ImpalaService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImpalaServiceImpl.class);
    private static final ImpalaConfig IMPALA_CONFIG =
            (ImpalaConfig) ConfigManager.getInstance().getBean(Constant.IMPALA_CONFIG);

    private LoginContext context;

    public ImpalaServiceImpl(LoginContext context) {
        this.context = context;
    }

    @Override
    public Connection getConnection(String url) throws ClassNotFoundException, SQLException {

        Class.forName("com.cloudera.impala.jdbc41.Driver");

        if (null == this.context.get()) {
            return DriverManager.getConnection(url);
        } else {
            final String iURL = url;
            Connection ret = this.context.get().doAs(new PrivilegedAction<Connection>() {
                @Override
                public Connection run() {
                    try {
                        return DriverManager.getConnection(iURL);
                    } catch (SQLException e) {
                        LOGGER.error("get connection from DriverManager error.", e);
                    }

                    return null;
                }
            });

            if (ret == null) {
                throw new SQLException("get secure impala connection error.");
            }

            return ret;
        }
    }

    @Override
    public void execute(String sql) {
        try (Connection connection = getConnection(IMPALA_CONFIG.getImpalaJDBCUrl())) {
            Statement statement = connection.createStatement();
            statement.execute(sql);
        } catch (ClassNotFoundException e) {
            throw new DataDivertorException("can not found impala JDBC Driver class.", e);
        } catch (SQLException e) {
            throw new DataDivertorException(String.format("execute sql [%s] failed.", sql), e);
        }
    }
}
