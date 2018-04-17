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

import com.cloudera.api.ApiRootResource;
import com.cloudera.api.v12.RootResourceV12;
import com.gridsum.datadivertor.exception.DataDivertorException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


public class ClouderaManagerUtils {

    public static RootResourceV12 getRootResource(ApiRootResource apiRootResource, String version) {
        RootResourceV12 rootResource = null;

        String methodGetRootV = "getRoot" + version.toUpperCase();
        try {
            Class apiRootResourceClass = ApiRootResource.class;
            Method method = apiRootResourceClass.getMethod(methodGetRootV, null);
            method.setAccessible(true);

            rootResource = (RootResourceV12) method.invoke(apiRootResource);
        } catch (NoSuchMethodException e) {
            throw new DataDivertorException(String.format("method [%s] can not found.", methodGetRootV), e);
        } catch (InvocationTargetException e) {
            throw new DataDivertorException(String.format("invoke method [%s] failed.", methodGetRootV), e);
        } catch (IllegalAccessException e) {
            throw new DataDivertorException(String.format("access method [%s] failed.", methodGetRootV), e);
        }

        return rootResource;
    }
}
