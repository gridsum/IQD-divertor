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

package com.gridsum.datadivertor.annotation;

import com.gridsum.datadivertor.configuration.AdvancedProperties;
import com.gridsum.datadivertor.configuration.ConfigManager;
import com.gridsum.datadivertor.exception.DataDivertorException;

import java.lang.reflect.Field;


public class AnnotationParser {

    /**
     * inject the properties to the corresponding filed of bean.
     *
     * @param beanProperties the bean classes.
     * @param configProperties the configuration items.
     */
    public static void inject(AdvancedProperties beanProperties, AdvancedProperties configProperties) {
        ConfigManager manager = ConfigManager.getInstance();
        String currentProperty = null;
        String currentFiledName = null;
        try {
            for (String property : beanProperties.stringPropertyNames()) {
                currentProperty = property;
                Class<?> clazz = Class.forName(property);
                Object bean = clazz.newInstance();
                for (Field field : clazz.getDeclaredFields()) {
                    currentFiledName = field.getName();
                    if (field.isAnnotationPresent(ConfigSet.class)) {
                        ConfigSet configSet = field.getAnnotation(ConfigSet.class);
                        if (!configProperties.containsKey(configSet.value())) {
                            throw new DataDivertorException(
                                    String.format("configuration item [%s] can not found.", configSet.value()));
                        }
                        field.setAccessible(true);
                        field.set(bean, configProperties.parsePropertyByType(
                                field.getType().getName(), configSet.value()));
                    }
                }

                manager.setBean(clazz.getSimpleName(), bean);
            }
        } catch (ClassNotFoundException e) {
            throw new DataDivertorException(String.format("class [%s] can not found.", currentProperty), e);
        } catch (InstantiationException e) {
            throw new DataDivertorException(String.format("instantiate class [%s] failed.", currentProperty), e);
        } catch (IllegalAccessException e) {
            throw new DataDivertorException(String.format("access field [%s] failed.", currentFiledName), e);
        }
    }
}
