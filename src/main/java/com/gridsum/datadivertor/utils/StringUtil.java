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

package com.gridsum.datadivertor.utils;

import com.google.common.base.Strings;

import java.util.regex.Pattern;


public class StringUtil {

    private static final Pattern INTEGER_PATTERN = Pattern.compile("^[-\\+]?[\\d]*$");
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("^[-\\+]?[\\d]*[.]?[\\d]*$");

    public static String nullToEmpty(String str) {
        return !Strings.isNullOrEmpty(str) ? str : "";
    }

    public static Long stringToLong(String str) {
        return !Strings.isNullOrEmpty(str) && INTEGER_PATTERN.matcher(str).matches() ? Long.parseLong(str) : -1L;
    }

    public static Double stringToDouble(String str) {
        return !Strings.isNullOrEmpty(str) && DECIMAL_PATTERN.matcher(str).matches() ? Double.parseDouble(str) : -1.0D;
    }
}
