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

import com.gridsum.datadivertor.exception.DataDivertorException;
import com.gridsum.datadivertor.service.HdfsService;
import com.gridsum.datadivertor.service.LoginContext;
import com.gridsum.datadivertor.utils.PathUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;


public class HdfsServiceImpl implements HdfsService {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsServiceImpl.class);

    private LoginContext context;
    private Configuration configuration;
    private FileSystem fileSystem;

    public HdfsServiceImpl(LoginContext context) {
        try {
            this.context = context;
            this.configuration = new Configuration();
            this.fileSystem = FileSystem.get(this.configuration);
        } catch (IOException e) {
            LOGGER.error("get HDFS file system error.");
        }
    }

    @Override
    public void fromLocalToHDFS(String fromFile, String toDir) {
        try {
            InputStream is = new BufferedInputStream(new FileInputStream(fromFile));

            File file = new File(fromFile);
            Path outputPath = new Path(PathUtil.makePath(toDir, file.getName()));
            OutputStream os = this.fileSystem.create(outputPath);

            IOUtils.copyBytes(is, os, this.configuration, true);

            file.delete();
        } catch (FileNotFoundException e) {
            throw new DataDivertorException(String.format("can not found %s.", fromFile), e);
        } catch (IOException e) {
            throw new DataDivertorException(String.format("upload %s to %s error.", fromFile, toDir), e);
        }
    }
}
