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
package gobblin.kafka.source.extractor.extract.kafka;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import kafka.common.Topic;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

import com.sun.tools.javac.util.Assert;

import lombok.extern.slf4j.Slf4j;

import gobblin.source.extractor.DataRecordException;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.kafka.KafkaTestBase;
import gobblin.configuration.State;
import gobblin.configuration.SourceState;
import gobblin.source.extractor.extract.kafka.KafkaSimpleStreamingExtractor;
import gobblin.source.extractor.extract.kafka.KafkaSimpleStreamingSource;
import gobblin.source.extractor.RecordEnvelope;
import gobblin.source.workunit.WorkUnit;
import gobblin.source.extractor.extract.LongWatermark;


import static org.mockito.Mockito.verify;


/**
 * Created by shshanka on 1/9/17.
 */
@Slf4j
public class KafkaSimpleStreamingTest {
  private final KafkaTestBase _kafkaTestHelper;
  public KafkaSimpleStreamingTest()
      throws InterruptedException, RuntimeException {
    _kafkaTestHelper = new KafkaTestBase();
  }

  @BeforeSuite
  public void beforeSuite() {
    log.warn("Process id = " + ManagementFactory.getRuntimeMXBean().getName());

    _kafkaTestHelper.startServers();
  }

  @AfterSuite
  public void afterSuite()
      throws IOException {
    try {
      _kafkaTestHelper.stopClients();
    }
    finally {
      _kafkaTestHelper.stopServers();
    }
  }

  @Test
  public void testSource()
      throws IOException, InterruptedException {
    String topic = "testSimpleStreamingSource";
    _kafkaTestHelper.provisionTopic(topic);
    SourceState ss = new SourceState();
    ss.setProp(ConfigurationKeys.KAFKA_BROKERS, "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    ss.setProp(KafkaSimpleStreamingSource.TOPIC_WHITELIST, topic);
    ss.setProp(ConfigurationKeys.JOB_NAME_KEY, topic);
    KafkaSimpleStreamingSource simpleSource = new KafkaSimpleStreamingSource();
    List<WorkUnit> lWu = simpleSource.getWorkunits(ss);
    Assert.check(lWu.size() == 1);
    WorkUnit wU = lWu.get(0);
    Assert.check(wU.getProp(KafkaSimpleStreamingSource.TOPIC_NAME).equals(topic));
    Assert.check(wU.getPropAsInt(KafkaSimpleStreamingSource.PARTITION_ID) == 0);
  }

  @Test
  public void testExtractor()
      throws IOException, InterruptedException, DataRecordException {
    final String topic = "testSimpleStreamingExtractor";
    _kafkaTestHelper.provisionTopic(topic);

    byte [] testData = {0, 1, 3};
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    Producer<String, byte[]> producer = new KafkaProducer<>(props);
    producer.send(new ProducerRecord<String, byte[]>(topic, topic, testData));
    producer.close();

    SourceState ss = new SourceState();
    ss.setProp(ConfigurationKeys.KAFKA_BROKERS, "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    ss.setProp(KafkaSimpleStreamingSource.TOPIC_WHITELIST, topic);
    ss.setProp(ConfigurationKeys.JOB_NAME_KEY, topic);
    KafkaSimpleStreamingSource simpleSource = new KafkaSimpleStreamingSource();

    List<WorkUnit> lWu = simpleSource.getWorkunits(ss);
    WorkUnit wU = lWu.get(0);
    WorkUnitState wSU = new WorkUnitState(wU, new State());
    wSU.setProp(ConfigurationKeys.KAFKA_BROKERS, "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    wSU.setProp(KafkaSimpleStreamingSource.TOPIC_WHITELIST, topic);
    wSU.setProp(ConfigurationKeys.JOB_NAME_KEY, topic);

    KafkaSimpleStreamingExtractor kSSE = new KafkaSimpleStreamingExtractor(wSU);
    TopicPartition tP = new TopicPartition(topic, 0);
    KafkaSimpleStreamingExtractor.KafkaWatermark kwm =
        new KafkaSimpleStreamingExtractor.KafkaWatermark(tP, new LongWatermark(0));
    byte [] reuse = new byte[1];
    RecordEnvelope<byte[]> oldRecord = new RecordEnvelope<>(reuse, kwm);
    RecordEnvelope<byte[]> record = kSSE.readRecord(oldRecord);
    Assert.check(Arrays.equals(record.getRecord(), testData));

    kSSE.commitWatermarks(Collections.singletonList(record.getWatermark()));

    Consumer<String, byte[]> kC = KafkaSimpleStreamingSource.getKafkaConsumer(wSU);
    OffsetAndMetadata oM = kC.committed(tP);
    Assert.check(record.getWatermark() instanceof KafkaSimpleStreamingExtractor.KafkaWatermark);
    KafkaSimpleStreamingExtractor.KafkaWatermark kWM = (KafkaSimpleStreamingExtractor.KafkaWatermark)record.getWatermark();
    Assert.check(oM.offset() == kWM.getLwm().getValue());
  }
}
