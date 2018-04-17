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

import com.gridsum.datadivertor.utils.BooleanUtil;
import com.gridsum.datadivertor.utils.IntegerUtil;
import com.gridsum.datadivertor.utils.PathUtil;
import com.gridsum.datadivertor.utils.StringUtil;
import junit.framework.TestCase;
import org.junit.BeforeClass;


public class UtilTest extends TestCase {

    @BeforeClass
    public void setUp() {
        System.setProperty("log.path", "./logs/");
    }

    public void testNullToFalse() {
        assertFalse( BooleanUtil.nullToFalse(null) );
    }

    public void testNullToZero() {
        assertEquals(new Long(0), IntegerUtil.nullToZero(null));
        assertEquals(new Long(1), IntegerUtil.nullToZero(1L));
    }

    public void testMakePath() {
        assertEquals("a/b/b", PathUtil.makePath("a", "b", "b"));
    }

    public void testStringToLong() {
        assertEquals(StringUtil.stringToLong("0"), new Long(0));
        assertEquals(StringUtil.stringToLong("1"), new Long(1));
        assertEquals(StringUtil.stringToLong("-1"), new Long(-1));
    }

    public void testNullToEmpty() {
        assertEquals(StringUtil.nullToEmpty("0"), "0");
        assertEquals(StringUtil.nullToEmpty(null), "");
    }

    public void testStringToDouble() {
        assertEquals(StringUtil.stringToDouble("0"), new Double(0));
        assertEquals(StringUtil.stringToDouble("1"), new Double(1));
        assertEquals(StringUtil.stringToDouble("-1"), new Double(-1));
    }
}
