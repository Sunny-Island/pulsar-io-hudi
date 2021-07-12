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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import lombok.AccessLevel;
import lombok.Getter;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import com.google.common.base.Preconditions;


/**
 * A source connector that generate randomized words.
 */
@Getter(AccessLevel.PACKAGE)
@Slf4j
public class HudiSink implements Sink<GenericRecord> {

    private HudiSinkConfig config;
    private HoodieJavaWriteClient hudiWriteClient;

    @Override
    public void open(Map<String, Object> map, SinkContext sinkContext) throws Exception {
        if (null != config) {
            throw new IllegalStateException("Connector is already open");
        }

        // load the configuration and validate it
        this.config = HudiSinkConfig.load(map);

        try{
            File schemaFile = new File(config.getSchemaPath());
            this.config.schema = new Schema.Parser().parse(schemaFile);
        }catch (IOException e){
            throw new RuntimeException(String.format("Failed to get schema from resource `%s`", config.getSchemaPath()));
        }

        this.config.validate();
        Configuration hadoopConf = new Configuration();
        Path path = new Path(config.getTablePath());
        FileSystem fs = FSUtils.getFs(config.getTablePath(), hadoopConf);
        if (!fs.exists(path)) {
            HoodieTableMetaClient.withPropertyBuilder()
                .setTableType(config.getIndexType())
                .setTableName(config.getTableName())
                .setPayloadClassName(HoodieAvroPayload.class.getName())
                .initTable(hadoopConf, config.getTablePath());
        }
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(config.getTablePath())
            .withSchema(this.config.schema.toString())
            .withParallelism(config.getInsertShuffleParallelism(), config.getUpsertShuffleParallelism())
            .withDeleteParallelism(2).forTable(config.getTableName())
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();
        this.hudiWriteClient = new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);
        log.info("Open HoodieJavaWriteClient successfully");
    }

    @Override
    public void write(Record<GenericRecord> record) throws Exception {

    }


    @Override
    public void close() throws Exception {

    }
}
