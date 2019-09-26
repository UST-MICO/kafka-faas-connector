/*
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
package io.github.ust.mico.kafkafaasconnector;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.ust.mico.kafkafaasconnector.configuration.KafkaConfig;
import io.github.ust.mico.kafkafaasconnector.kafka.CloudEventDeserializer;
import io.github.ust.mico.kafkafaasconnector.kafka.CloudEventSerializer;
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static io.github.ust.mico.kafkafaasconnector.TestConstants.DEFAULT_KAFKA_POLL_TIMEOUT;

public class MicoKafkaTestHelper {

    /**
     * Unsubscribes and closes the consumer. Needed after each test.
     *
     * @param consumer
     */
    public static void unsubscribeConsumer(Consumer consumer) {
        consumer.unsubscribe();
        consumer.close();
    }

    private EmbeddedKafkaBroker embeddedKafka;
    private KafkaConfig kafkaConfig;
    private KafkaTemplate<String, MicoCloudEventImpl<JsonNode>> template;

    public MicoKafkaTestHelper(EmbeddedKafkaBroker embeddedKafka, KafkaConfig kafkaConfig) {
        this.embeddedKafka = embeddedKafka;
        this.kafkaConfig = kafkaConfig;
        this.template = this.getKafkaProducer();
    }


    public KafkaTemplate<String, MicoCloudEventImpl<JsonNode>> getTemplate() {
        return template;
    }

    /**
     * Get a kafka consumer for testing with an embedded broker.
     * <p>
     * The consumer needs to be unsubscribed after the test is finished.
     * If not other tests using the same broker may fail to get messages!
     *
     * @return
     */
    public Consumer<String, MicoCloudEventImpl<JsonNode>> getKafkaConsumer() {
        Map<String, Object> consumerProps = org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps("testT", "false", embeddedKafka);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            ErrorHandlingDeserializer2.class);
        consumerProps.put(ErrorHandlingDeserializer2.KEY_DESERIALIZER_CLASS,
            StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ErrorHandlingDeserializer2.class);
        consumerProps.put(ErrorHandlingDeserializer2.VALUE_DESERIALIZER_CLASS,
            CloudEventDeserializer.class);
        DefaultKafkaConsumerFactory<String, MicoCloudEventImpl<JsonNode>> cf = new DefaultKafkaConsumerFactory<String, MicoCloudEventImpl<JsonNode>>(consumerProps);
        return cf.createConsumer();
    }


    /**
     * Get a kafka producer (KafkaTemplate) for testing with an embedded broker.
     *
     * @return
     */
    public KafkaTemplate<String, MicoCloudEventImpl<JsonNode>> getKafkaProducer() {
        Map<String, Object> producerProps = org.springframework.kafka.test.utils.KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class);
        producerProps.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            CloudEventSerializer.class);
        DefaultKafkaProducerFactory<String, MicoCloudEventImpl<JsonNode>> pf = new DefaultKafkaProducerFactory<String, MicoCloudEventImpl<JsonNode>>(producerProps);
        return new KafkaTemplate<>(pf);
    }

    /**
     * This method requests the topics which are acctually used by the broker.
     * It is nesseary because embeddedKafka.getTopics() does not contain a recent topic list
     *
     * @return
     */
    public Set<String> requestActuallySetTopics() {
        Set<String> topics = new HashSet<>();
        embeddedKafka.doWithAdmin(admin -> {
            try {
                topics.addAll(admin.listTopics().names().get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
        return topics;
    }

    /**
     * Contains all topics which are necessary for the tests
     *
     * @return
     */
    public Set<String> getRequiredTopics() {
        Set<String> requiredTopics = new HashSet<>();
        requiredTopics.addAll(Arrays.asList(
            kafkaConfig.getTestMessageOutputTopic(),
            kafkaConfig.getDeadLetterTopic(),
            kafkaConfig.getInvalidMessageTopic(),
            kafkaConfig.getInputTopic(),
            kafkaConfig.getOutputTopic(),
            TestConstants.ROUTING_TOPIC_1,
            TestConstants.ROUTING_TOPIC_2,
            TestConstants.ROUTING_TOPIC_3,
            TestConstants.ROUTING_TOPIC_4));
        return requiredTopics;
    }

    /**
     * Generates a consumer based on the given topics
     *
     * @return
     */
    public Consumer<String, MicoCloudEventImpl<JsonNode>> getKafkaConsumer(String... topics) {
        Consumer<String, MicoCloudEventImpl<JsonNode>> consumer = this.getKafkaConsumer();
        embeddedKafka.consumeFromEmbeddedTopics(consumer, topics);
        return consumer;
    }

    /**
     * Exchanges a message. It sends the provided message to the default input topic and waits {@link TestConstants#DEFAULT_KAFKA_POLL_TIMEOUT} long for a reply on the provided topic.
     *
     * @param consumer
     * @param topic
     * @param cloudEventSimple
     * @return
     */
    public ConsumerRecord<String, MicoCloudEventImpl<JsonNode>> exchangeMessage(Consumer<String, MicoCloudEventImpl<JsonNode>> consumer, String topic, MicoCloudEventImpl<JsonNode> cloudEventSimple) {
        template.send(kafkaConfig.getInputTopic(), cloudEventSimple);
        return KafkaTestUtils.getSingleRecord(consumer, topic, DEFAULT_KAFKA_POLL_TIMEOUT);
    }

    /**
     * Consumes all messages send during the test execution
     *
     * @param consumer the {@link Consumer Kafka Consumer}
     * @return the consumed messages
     */
    public static ArrayList<ConsumerRecord<String, MicoCloudEventImpl<JsonNode>>> consumeAllMessages(Consumer<String, MicoCloudEventImpl<JsonNode>> consumer) {
        ArrayList<ConsumerRecord<String, MicoCloudEventImpl<JsonNode>>> events = new ArrayList<>();

        int lastSize = events.size();
        while (events.size() == 0 || lastSize < events.size()) {
            lastSize = events.size();
            KafkaTestUtils.getRecords(consumer, 1000).forEach(events::add);
        }
        return events;
    }
}
