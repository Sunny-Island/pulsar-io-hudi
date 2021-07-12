/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.hudi;

import static org.junit.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Unit test {@link HudiSinkConfig}.
 */
public class HudiSinkConfigTest {

    /**
     * Test Case: load the configuration from an empty property map.
     *
     * @throws IOException when failed to load the property map
     */
    @Test
    public void testLoadEmptyPropertyMap() throws IOException {
        Map<String, Object> emptyMap = Collections.emptyMap();
        HudiSinkConfig config = HudiSinkConfig.load(emptyMap);
        assertNull("TableName should not be set", config.getTableName());
        assertNull("TablePath should not be set", config.getTablePath());
    }

    /**
     * Test Case: load the configuration from a property map.
     *
     * @throws IOException when failed to load the property map
     */
    @Test
    public void testLoadPropertyMap() throws IOException {
        Map<String, Object> properties = new HashMap<>();
        long seed = System.currentTimeMillis();
        properties.put("tablePath", "file:///tmp/hoodie/sample-table");
        properties.put("tableName", "test-table");

        HudiSinkConfig config = HudiSinkConfig.load(properties);

        assertEquals("file:///tmp/hoodie/sample-table", config.getTablePath());
        assertEquals("test-table", config.getTableName());
    }

    /**
     * Test Case: load property config from .yaml file
     *
     * @throws IOException when failed to load the property map
     */
     @Test
     public void testLoadFromYaml() throws IOException{
         ClassLoader classLoader = getClass().getClassLoader();
         File file = new File(classLoader.getResource("sinkConfig.yaml").getFile());
         String path = file.getAbsolutePath();
         HudiSinkConfig config = HudiSinkConfig.load(path);
         assertEquals(config.getTableName(), "test-table");
         assertEquals(config.getTableType(), "MERGE_ON_READ");
     }

     @Test(expected = IllegalArgumentException.class)
     public void testValidate() throws IOException{
         Map<String, Object> properties = new HashMap<>();
         long seed = System.currentTimeMillis();
         properties.put("tablePath", "file:///tmp/hoodie/sample-table");
         properties.put("tableName", "test-table");
         properties.put("tableType", "TEST_TYPE");
         HudiSinkConfig config = HudiSinkConfig.load(properties);

         config.validate();
     }
}
