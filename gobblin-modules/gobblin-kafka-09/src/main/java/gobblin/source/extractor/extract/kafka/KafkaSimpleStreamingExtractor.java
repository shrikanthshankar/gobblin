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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.util.hash.Hash;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import kafka.Kafka;
import kafka.common.TopicAndPartition;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.configuration.State;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.Watermark;
import gobblin.source.extractor.WatermarkSerializerHelper;
import gobblin.source.extractor.extract.EventBasedExtractor;
import gobblin.source.extractor.StreamingExtractor;
import gobblin.source.extractor.RecordEnvelope;
import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.source.extractor.ComparableWatermark;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.metrics.Tag;
import gobblin.source.extractor.extract.kafka.SimpleKafkaSchemaRegistry;
import gobblin.writer.WatermarkStorage;


/**
 * An implementation of {@link StreamingExtractor} from which reads from Kafka and returns records as an array of bytes.
 *
 * @author Shrikanth Shankar
 *
 *
 */
public class KafkaSimpleStreamingExtractor extends EventBasedExtractor<String, RecordEnvelope<byte[]>> implements StreamingExtractor<String, byte[]>, WatermarkStorage {

  public static class KafkaWatermark implements CheckpointableWatermark {
    TopicPartition _topicPartition;
    LongWatermark _lwm;

    KafkaWatermark(TopicPartition topicPartition, LongWatermark lwm) {
      _topicPartition = topicPartition;
      _lwm = lwm;
    }

    @Override
    public String getSource() {
      return _topicPartition.toString();
    }

    @Override
    public ComparableWatermark getWatermark() {
      return _lwm;
    }

    @Override
    public short calculatePercentCompletion(Watermark lowWatermark, Watermark highWatermark) {
      return 0;
    }

    @Override
    public JsonElement toJson() {
      return return WatermarkSerializerHelper.convertWatermarkToJson(this);;
    }

    @Override
    public int compareTo(CheckpointableWatermark o) {
      Preconditions.checkArgument(o instanceof KafkaWatermark);
      KafkaWatermark ko = (KafkaWatermark)o;
      Preconditions.checkArgument(_topicPartition.equals(ko._topicPartition));
      return _lwm.compareTo(ko._lwm);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null)
        return false;
      return this.compareTo((CheckpointableWatermark)obj) == 0;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      return _topicPartition.hashCode() * prime + _lwm.hashCode();
    }

    public TopicPartition getTopicPartition() {return _topicPartition;};

    public LongWatermark getLwm() {return  _lwm;};
  }


  private final KafkaSchemaRegistry<String, String> _kafkaSchemaRegistry;
  private Consumer<String, byte[]> _consumer;
  private TopicPartition _partition;
  private Iterator<ConsumerRecord<String, byte[]>> _records;
  long _rowCount = 0;

  public KafkaSimpleStreamingExtractor(WorkUnitState state) {
    super(state);
    _kafkaSchemaRegistry = new SimpleKafkaSchemaRegistry(state.getProperties());
    _consumer = KafkaSimpleStreamingSource.getKafkaConsumer(state);
    closer.register(_consumer);
    _partition = new TopicPartition(state.getProp(KafkaSimpleStreamingSource.TOPIC_NAME),
                                    state.getPropAsInt(KafkaSimpleStreamingSource.PARTITION_ID));
    _consumer.assign(Collections.singletonList(_partition));
    OffsetAndMetadata offset = _consumer.committed(_partition);
    if (offset == null)
      _consumer.seekToBeginning(_partition);
    else
      _consumer.seek(_partition, offset.offset());
  }

  /**
  * Get the schema (metadata) of the extracted data records.
  *
  * @return the Kafka topic being extracted
  * @throws IOException if there is problem getting the schema
  */
  @Override
  public String getSchema() throws IOException {
    try {
      return this._kafkaSchemaRegistry.getLatestSchemaByTopic(this._partition.topic());
    } catch (SchemaRegistryException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Tag<?>> generateTags(State state) {
    List<Tag<?>> tags = super.generateTags(state);
    tags.add(new Tag<>("kafkaTopic", state.getProp(KafkaSimpleStreamingSource.TOPIC_WHITELIST)));
    return tags;
  }

  /**
   * Return the next record when available. Will never time out since this is a streaming source.
   * TODO: Add interrupt so Gobblin can shut down the source.
   */
  @Override
  public RecordEnvelope<byte[]> readRecordImpl(RecordEnvelope<byte[]> reuse) throws DataRecordException, IOException {
    while ((_records == null) || (!_records.hasNext())) {
      _records = _consumer.poll(100).iterator();
    }
    ConsumerRecord<String, byte[]> record = _records.next();
    _rowCount++;
    return new RecordEnvelope<byte[]>(record.value(), new KafkaWatermark(_partition, new LongWatermark(record.offset())));
  }

  @Override
  public long getExpectedRecordCount() {
    return _rowCount;
  }

  @Override
  public void close() throws IOException {
    closer.close();
  }

  @Deprecated
  @Override
  public long getHighWatermark() {
    return 0;
  }

  @Override
  public void commitWatermarks(Iterable<CheckpointableWatermark> watermarks) {
    Map<TopicPartition, OffsetAndMetadata> wmToCommit = new HashMap<TopicPartition, OffsetAndMetadata>();
    for (CheckpointableWatermark cwm : watermarks) {
      Preconditions.checkArgument(cwm instanceof KafkaWatermark);
      KafkaWatermark kwm = ((KafkaWatermark)cwm);
      wmToCommit.put(kwm.getTopicPartition(), new OffsetAndMetadata(kwm.getLwm().getValue()));
    }
    _consumer.commitSync(wmToCommit);
  }
}
