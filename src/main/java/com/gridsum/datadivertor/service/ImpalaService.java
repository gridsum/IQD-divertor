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

package com.gridsum.datadivertor.service;


import java.sql.Connection;
import java.sql.SQLException;

public interface ImpalaService {

    /**
     * get a JDBC connection from a designated url.
     *
     * @param url the url to be connected.
     * @return Connection object to the designated url.
     * @throws ClassNotFoundException,SQLException
     */
    Connection getConnection(String url) throws ClassNotFoundException, SQLException;

    /**
     * execute a designated DDL or DML sql.
     *
     * @param sql the designated sql to be executed.
     */
    void execute(String sql);
}
