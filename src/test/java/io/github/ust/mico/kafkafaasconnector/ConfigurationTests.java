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
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;
import org.junit.*;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import io.github.ust.mico.kafkafaasconnector.configuration.OpenFaaSConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.PostConstruct;

import java.util.ArrayList;
import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;


@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@EnableAutoConfiguration
@AutoConfigureMockMvc
@DirtiesContext
@Slf4j
public class ConfigurationTests {

    @Autowired
    private OpenFaaSConfig openFaaSConfig;

    @Autowired
    private KafkaConfig kafkaConfig;

    private final EmbeddedKafkaBroker embeddedKafka = broker.getEmbeddedKafka();

//     https://docs.spring.io/spring-kafka/docs/2.2.6.RELEASE/reference/html/#kafka-testing-junit4-class-rule
    @ClassRule
    public static EmbeddedKafkaRule broker = new EmbeddedKafkaRule(1, false);

    private KafkaTemplate<String, MicoCloudEventImpl<JsonNode>> template;
    private MicoKafkaTestHelper micoKafkaTestHelper;

    @ClassRule
    public static final EnvironmentVariables environmentVariables = new EnvironmentVariables()
        .set("OPENFAAS_FUNCTION_NAME", "config_openfaas_func")
        .set("OPENFAAS_GATEWAY","config_openfaas_gateway")
        .set("KAFKA_GROUP_ID", "kafka_group_id")
        .set("KAFKA_TOPIC_INPUT", "config_kafka_in")
        .set("KAFKA_TOPIC_INVALID_MESSAGE", "config_kafka_invalid")
        .set("KAFKA_TOPIC_OUTPUT", "config_kafka_out")
        .set("KAFKA_TOPIC_DEAD_LETTER", "config_kafka_dead_letter")
        .set("KAFKA_TOPIC_TEST_MESSAGE_OUTPUT", "test_msg");

    @PostConstruct
    public void before(){
        this.micoKafkaTestHelper = new MicoKafkaTestHelper(embeddedKafka,kafkaConfig);
        template = this.micoKafkaTestHelper.getTemplate();
        //We need to add them outside of the rule because the autowired kakfaConfig is not accessible from the static rule
        //We can not use @BeforeClass which is only executed once because it has to be static and we do not have access to the autowired kakfaConfig

        Set<String> requiredTopics = this.micoKafkaTestHelper.getRequiredTopics();
        Set<String> alreadySetTopics = this.micoKafkaTestHelper.requestActuallySetTopics();
        requiredTopics.removeAll(alreadySetTopics);
        requiredTopics.forEach(topic -> embeddedKafka.addTopics(topic));
    }

    @Test
    public void testOpenFaaSConfig() {
        assertThat(System.getenv("OPENFAAS_FUNCTION_NAME"), is(openFaaSConfig.getFunctionName()));
        assertThat(System.getenv("OPENFAAS_GATEWAY"), is(openFaaSConfig.getGateway()));
    }

    @Test
    public void testKafkaConfig() {
        assertThat(System.getenv("KAFKA_GROUP_ID"), is(kafkaConfig.getGroupId()));
        assertThat(System.getenv("KAFKA_TOPIC_INPUT"), is(kafkaConfig.getInputTopic()));
        assertThat(System.getenv("KAFKA_TOPIC_OUTPUT"), is(kafkaConfig.getOutputTopic()));
        assertThat(System.getenv("KAFKA_TOPIC_INVALID_MESSAGE"), is(kafkaConfig.getInvalidMessageTopic()));
        assertThat(System.getenv("KAFKA_TOPIC_DEAD_LETTER"), is(kafkaConfig.getDeadLetterTopic()));
        assertThat(System.getenv("KAFKA_TOPIC_TEST_MESSAGE_OUTPUT"), is(kafkaConfig.getTestMessageOutputTopic()));
    }
}
