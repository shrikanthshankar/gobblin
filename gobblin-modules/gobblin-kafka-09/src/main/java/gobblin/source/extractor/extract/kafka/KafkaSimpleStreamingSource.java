/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.Tag;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.RecordEnvelope;
import gobblin.source.extractor.extract.EventBasedSource;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.source.extractor.Extractor;
import gobblin.configuration.SourceState;


/**
 * A {@link Source} implementation for a simple streaming kafka extractor.
 *
 * @author Shrikanth Shankar
 *
 */
public class KafkaSimpleStreamingSource extends EventBasedSource<String, RecordEnvelope<byte[]>> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSimpleStreamingSource.class);

  public static final String TOPIC_WHITELIST = "gobblin.streaming.kafka.topic";
  public static final String TOPIC_NAME = "gobblin.streaming.topic.name";
  public static final String PARTITION_ID = "gobblin.streaming.partition.id";
  private final Closer closer = Closer.create();
  public static final Extract.TableType DEFAULT_TABLE_TYPE = Extract.TableType.APPEND_ONLY;
  public static final String DEFAULT_NAMESPACE_NAME = "KAFKA";

  static public Consumer<String, byte[]> getKafkaConsumer(State state) {

    List<String> brokers = state.getPropAsList(ConfigurationKeys.KAFKA_BROKERS);
    Properties props = new Properties();
    props.put("bootstrap.servers", String.join(",", brokers));
    props.put("group.id", state.getProp(ConfigurationKeys.JOB_NAME_KEY));
    props.put("enable.auto.commit", "false");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    Consumer consumer = null;
    try {
      consumer = new KafkaConsumer<>(props);
    } catch (Exception e) {
      LOG.error("Exception when creating Kafka consumer - {}", e);
      throw Throwables.propagate(ioe);
    }
    return consumer;
  }

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
      Consumer<String, byte[]> consumer = getKafkaConsumer(state);
      LOG.debug("Consumer is {}", consumer);
      String topic = state.getProp(TOPIC_WHITELIST); // TODO: fix this to use the new API when KafkaWrapper is fixed
      List<WorkUnit> workUnits = new ArrayList<WorkUnit>();
      List<PartitionInfo> topicPartitions;
      topicPartitions = consumer.partitionsFor(topic);
      LOG.debug("Partition count is {}", topicPartitions.size());
      for (PartitionInfo topicPartition : topicPartitions){
        LOG.debug("Partition info is {}", topicPartition);
        Extract extract = this.createExtract(DEFAULT_TABLE_TYPE, DEFAULT_NAMESPACE_NAME, topicPartition.topic());
        WorkUnit workUnit = WorkUnit.create(extract);
        workUnit.setProp(TOPIC_NAME, topicPartition.topic());
        workUnit.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, topicPartition.topic());
        workUnit.setProp(PARTITION_ID, topicPartition.partition());
        workUnits.add(workUnit);
      }
      return workUnits;
    }


  @Override
  public Extractor<String, RecordEnvelope<byte[]>> getExtractor(WorkUnitState state) throws IOException {
    return new KafkaSimpleStreamingExtractor(state);
    }
}
