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


import com.gridsum.datadivertor.exception.DataDivertorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class AdvancedProperties extends Properties {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdvancedProperties.class);

    /**
     * parse the property value by field type.
     *
     * @param type the field type.
     * @param key the property name.
     * @return Object object contains the value of property.
     */
    public Object parsePropertyByType(String type, String key) {
        String value = this.getProperty(key);
        Object obj;
        try {
            if ("int".equals(type)) {
                obj = Integer.parseInt(value);
            } else if ("long".equals(type)) {
                obj = Long.parseLong(value);
            } else if ("float".equals(type)) {
                obj = Float.parseFloat(value);
            } else if ("double".equals(type)) {
                obj = Double.parseDouble(value);
            } else if ("boolean".equals(type)) {
                obj = Boolean.parseBoolean(value);
            } else {
                obj = value;
            }
        } catch (Exception e) {
            throw new DataDivertorException(String.format("parse the value of %s from properties file error", key), e);
        }

        return obj;
    }

    /**
     * read properties from designated file.
     *
     * @param fileName the name of properties file.
     * @return AdvancedProperties object.
     */
    public static AdvancedProperties read(String fileName) {
        try (FileInputStream inputStream = new FileInputStream(fileName)) {
            return read(inputStream);
        } catch (IOException e) {
            throw new DataDivertorException(String.format("read properties file [%s] failed.", fileName), e);
        }
    }

    /**
     * read properties from designated input stream.
     *
     * @param inputStream the input stream of properties file.
     * @return AdvancedProperties object.
     */
    public static AdvancedProperties read(InputStream inputStream) {
        AdvancedProperties properties = new AdvancedProperties();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            throw new DataDivertorException("load properties file failed.", e);
        }

        return properties;
    }
}
