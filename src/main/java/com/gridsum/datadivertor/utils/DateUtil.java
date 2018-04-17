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


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;


public class DateUtil {

    public static final SimpleDateFormat SDF_DATETIME = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    public static final SimpleDateFormat SDF_DATETIME_TO_LONG = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    private static final int CST_TO_GMT = -8;
    private static final int GMT_TO_CST = 8;


    public static Date cst2gmt(Date cst) {
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTime(cst);
        calendar.add(Calendar.HOUR_OF_DAY, CST_TO_GMT);
        return calendar.getTime();
    }

    public static Date gmt2cst(Date gmt) {
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTime(gmt);
        calendar.add(Calendar.HOUR_OF_DAY, GMT_TO_CST);
        return calendar.getTime();
    }

    public static String dateToString(Date date, SimpleDateFormat sdf) {
        return date != null ? sdf.format(date) : "";
    }

    public static Date stringToDate(String dateStr, SimpleDateFormat sdf) throws ParseException {
        return sdf.parse(dateStr);
    }

    public static long dateToLong(Date date, SimpleDateFormat sdf) {
        return date != null ? Long.parseLong(dateToString(date, sdf)) : 0;
    }

    public static Date dayStart(Date date) {
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTime(date);

        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        return calendar.getTime();
    }
}
