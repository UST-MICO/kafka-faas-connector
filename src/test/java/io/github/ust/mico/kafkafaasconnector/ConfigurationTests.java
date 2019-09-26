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

import io.github.ust.mico.kafkafaasconnector.configuration.KafkaConfig;
import org.junit.*;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import io.github.ust.mico.kafkafaasconnector.configuration.OpenFaaSConfig;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;


@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@EnableAutoConfiguration
@ActiveProfiles("testing")
public class ConfigurationTests {

    @Autowired
    private OpenFaaSConfig openFaaSConfig;

    @Autowired
    private KafkaConfig kafkaConfig;

    // https://docs.spring.io/spring-kafka/docs/2.2.6.RELEASE/reference/html/#kafka-testing-junit4-class-rule
    @ClassRule
    public static EmbeddedKafkaRule broker = new EmbeddedKafkaRule(1, false);


    @ClassRule
    public static final EnvironmentVariables environmentVariables = new EnvironmentVariables()
        .set("OPENFAAS_FUNCTION_NAME", "openfaas_name")
        .set("OPENFAAS_GATEWAY","openfaas_gateway")
        .set("KAFKA_TOPIC_INPUT", "kafka_in")
        .set("KAFKA_TOPIC_INVALID_MESSAGE", "kafka_invalid")
        .set("KAFKA_TOPIC_OUTPUT", "kafka_out")
        .set("KAFKA_TOPIC_DEAD_LETTER", "kafka_dead_letter")
        .set("KAFKA_TOPIC_TEST_MESSAGE_OUTPUT", "kafka_test_msg");

    @Test
    public void testOpenFaaSConfig() {
        // Tests if the environment variables are injected into OpenFaaSConfig
        assertThat(System.getenv("OPENFAAS_FUNCTION_NAME"), is(openFaaSConfig.getFunctionName()));
        assertThat(System.getenv("OPENFAAS_GATEWAY"), is(openFaaSConfig.getGateway()));
    }

    @Test
    public void testKafkaConfig() {
        // Tests if the environment variables are injected into KafkaConfig
        assertTrue(kafkaConfig.getGroupId().length() == 36);
        assertThat(System.getenv("KAFKA_TOPIC_INPUT"), is(kafkaConfig.getInputTopic()));
        assertThat(System.getenv("KAFKA_TOPIC_OUTPUT"), is(kafkaConfig.getOutputTopic()));
        assertThat(System.getenv("KAFKA_TOPIC_INVALID_MESSAGE"), is(kafkaConfig.getInvalidMessageTopic()));
        assertThat(System.getenv("KAFKA_TOPIC_DEAD_LETTER"), is(kafkaConfig.getDeadLetterTopic()));
        assertThat(System.getenv("KAFKA_TOPIC_TEST_MESSAGE_OUTPUT"), is(kafkaConfig.getTestMessageOutputTopic()));
    }
}
