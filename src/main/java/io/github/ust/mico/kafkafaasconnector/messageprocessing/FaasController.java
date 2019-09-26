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

package io.github.ust.mico.kafkafaasconnector.messageprocessing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.cloudevents.json.Json;
import io.github.ust.mico.kafkafaasconnector.configuration.OpenFaaSConfig;
import io.github.ust.mico.kafkafaasconnector.exception.MicoCloudEventException;
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class FaasController {

    @Autowired
    private CloudEventManipulator cloudEventManipulator;

    @Autowired
    private OpenFaaSConfig openFaaSConfig;

    @Autowired
    private RestTemplate restTemplate;

    /**
     * Synchronously call the configured openFaaS function.
     *
     * @param cloudEvent the cloud event used as parameter for the function
     * @return the result of the function call (in serialized form)
     */
    public List<MicoCloudEventImpl<JsonNode>> callFaasFunction(MicoCloudEventImpl<JsonNode> cloudEvent) throws MicoCloudEventException {
        if (this.openFaaSConfig.isSkipFunctionCall() || this.openFaaSConfig.getFunctionName().equals("$$DEFAULT$$") || this.openFaaSConfig.getFunctionName().isEmpty()) {
            log.debug("Skip faas function call. Function name '{}'",this.openFaaSConfig.getFunctionName());
            return Collections.singletonList(cloudEvent);
        }
        URL functionUrl = null;
        try {
            functionUrl = openFaaSConfig.getFunctionUrl();
            log.debug("Start request to function '{}'", functionUrl.toString());
            String cloudEventSerialized = Json.encode(cloudEventManipulator.updateRouteHistoryWithFunctionCall(cloudEvent, openFaaSConfig.getFunctionName()));
            log.debug("Serialized cloud event: {}", cloudEventSerialized);
            String result = restTemplate.postForObject(functionUrl.toString(), cloudEventSerialized, String.class);
            log.debug("Faas call resulted in: '{}'", result);
            return parseFunctionResult(result, cloudEvent);
        } catch (MalformedURLException e) {
            throw new MicoCloudEventException("Failed to call faas-function. Caused by: " + e.getMessage(), cloudEvent);
        } catch (IllegalStateException e) {
            log.error("Failed to serialize CloudEvent '{}'.", cloudEvent);
            throw new MicoCloudEventException("Failed to serialize CloudEvent while calling the faas-function.", cloudEvent);
        } catch (HttpStatusCodeException e) {
            log.error("FaaS function '{}' returned http status code '{}'. Expected 200 OK.", functionUrl, e.getStatusCode());
            throw new MicoCloudEventException(e.toString(), cloudEvent);
        }
    }

    /**
     * Parse the result of a faas function call.
     *
     * @param sourceCloudEvent only used for better error messages
     * @return an ArrayList of cloud events
     */
    public ArrayList<MicoCloudEventImpl<JsonNode>> parseFunctionResult(String functionResult, MicoCloudEventImpl<JsonNode> sourceCloudEvent) throws MicoCloudEventException {
        try {
            return Json.decodeValue(functionResult, new TypeReference<ArrayList<MicoCloudEventImpl<JsonNode>>>() {
            });
        } catch (IllegalStateException e) {
            log.error("Failed to parse JSON from response '{}'.", functionResult);
            throw new MicoCloudEventException("Failed to parse JSON from response from the faas-function.", sourceCloudEvent);
        }
    }

}
