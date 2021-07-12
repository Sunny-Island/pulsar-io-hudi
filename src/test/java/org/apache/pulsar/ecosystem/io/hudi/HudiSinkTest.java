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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.io.core.SourceContext;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link HudiSink}.
 */
public class HudiSinkTest {

    private final Map<String, Object> goodConfig = new HashMap<>();
    private final Map<String, Object> badConfig = new HashMap<>();

    @Before
    public void setup() {
        goodConfig.put("randomSeed", System.currentTimeMillis());
        goodConfig.put("maxMessageSize", 1024);
    }

   @Test
   public void singleWrite(){
   }

}
