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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import org.apache.avro.Schema;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.index.HoodieIndex;


/**
 * The configuration class for {@link HudiSink}.
 */
@Getter
@EqualsAndHashCode
@ToString
public class HudiSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String tableType = "COPY_ON_WRITE";

    private String tablePath;

    private String tableName;

    private int insertShuffleParallelism = 2;

    private int upsertShuffleParallelism = 2;

    private int deleteParallelism = 2;

    private String indexType = "INMEMORY";

    private String schemaPath;

    public Schema schema;

    /**
     * Validate if the configuration is valid.
     */
    public void validate() {

        Objects.requireNonNull(tablePath, "No `tablePath` is provided");
        Objects.requireNonNull(tableName, "No `tableName` is provided");
        Objects.requireNonNull(schema, "No schema is loaded");
        Preconditions.checkArgument(validateTableType(tableType),"invalid `tableType`:" + tableType);
        Preconditions.checkArgument(validateIndexType(indexType),"invalid `tableType`:" + tableType);
    }

    /**
     * Load the configuration from provided properties.
     *
     * @param config property map
     * @return a loaded {@link HudiSinkConfig}.
     * @throws IOException when fail to load the configuration from provided properties
     */
    public static HudiSinkConfig load(Map<String, Object> config) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(config), HudiSinkConfig.class);
    }

    public static HudiSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), HudiSinkConfig.class);
    }

    /**
     * Valid options: COPY_ON_WRITE, MERGE_ON_READ;
     *
     * @param tableType
     * @return if tableType is valid
     */
    private boolean validateTableType(String tableType){
        for (HoodieTableType type :HoodieTableType.values()){
            if (type.name().equals(tableType)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Valid options:
     *         HBASE,
     *         INMEMORY,
     *         BLOOM,
     *         GLOBAL_BLOOM,
     *         SIMPLE,
     *         GLOBAL_SIMPLE
     *
     * @param indexType
     * @return if indexType is valid
     */
    private boolean validateIndexType(String indexType){
        for (HoodieIndex.IndexType type :HoodieIndex.IndexType.values()){
            if (type.name().equals(indexType)) {
                return true;
            }
        }
        return false;
    }
}
