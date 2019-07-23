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

package io.github.ust.mico.kafkafaasconnector.kafka;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.cloudevents.CloudEvent;
import io.cloudevents.Extension;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;


/**
 * For more information read https://mico-docs.readthedocs.io/en/latest/messaging/cloudevents.html
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
@JsonDeserialize(as = MicoCloudEventImpl.class)
@JsonNaming(value = PropertyNamingStrategy.LowerCaseStrategy.class)
public class MicoCloudEventImpl<T> implements CloudEvent<T> {

    protected static final String SPEC_VERSION = "0.2";

    //mandatory
    private String id;
    private URI source;
    private String type;
    private String specVersion = SPEC_VERSION;


    //Optional
    private ZonedDateTime time;
    private URI schemaURL;
    private String contentType;
    private T data;
    @JsonIgnore
    private Map<String, JsonNode> extensionsMap = new HashMap<>();

    private String correlationId;
    private String createdFrom;
    private List<RouteHistory> route;
    private LinkedList<List<String>> routingSlip;
    private Boolean isTestMessage;
    private String filterOutBeforeTopic;
    private Boolean isErrorMessage;
    private String errorMessage;
    private String errorTrace;
    private ZonedDateTime expiryDate;
    private String sequenceId;
    private Integer sequenceNumber;
    private Integer sequenceSize;
    private String returnTopic;
    private String dataRef;
    private String subject;

    /**
     * Copy constructor providing a shallow copy of the cloud event.
     *
     * @param cloudEvent the event to copy
     */
    public MicoCloudEventImpl(MicoCloudEventImpl<T> cloudEvent) {
        this(
            cloudEvent.id,
            cloudEvent.source,
            cloudEvent.type,
            cloudEvent.specVersion,
            cloudEvent.time,
            cloudEvent.schemaURL,
            cloudEvent.contentType,
            cloudEvent.data,
            cloudEvent.extensionsMap,
            cloudEvent.correlationId,
            cloudEvent.createdFrom,
            cloudEvent.route,
            cloudEvent.routingSlip,
            cloudEvent.isTestMessage,
            cloudEvent.filterOutBeforeTopic,
            cloudEvent.isErrorMessage,
            cloudEvent.errorMessage,
            cloudEvent.errorTrace,
            cloudEvent.expiryDate,
            cloudEvent.sequenceId,
            cloudEvent.sequenceNumber,
            cloudEvent.sequenceSize,
            cloudEvent.returnTopic,
            cloudEvent.dataRef,
            cloudEvent.subject
        );
    }

    public MicoCloudEventImpl<T> setRandomId() {
        id = UUID.randomUUID().toString();
        return this;
    }


    public MicoCloudEventImpl<T> setBaseCloudEvent(MicoCloudEventImpl<T> cloudEvent) {
        id = cloudEvent.getId();
        specVersion = cloudEvent.getSpecVersion();
        source = cloudEvent.getSource();
        type = cloudEvent.getType();
        data = cloudEvent.getData().orElse(null);
        contentType = cloudEvent.getContentType().orElse(null);
        schemaURL = cloudEvent.getSchemaURL().orElse(null);
        extensionsMap = cloudEvent.getExtensionsAsMap().orElse(new HashMap<String, JsonNode>());
        time = cloudEvent.getTime().orElse(null);
        return this;
    }

    @JsonAnySetter
    public void setExtension(String key, JsonNode value) {
        extensionsMap.put(key, value);
    }

    @JsonAnyGetter
    public Map<String, JsonNode> getExtensionsForSerializer() {
        return extensionsMap;
    }

    public Optional<ZonedDateTime> getTime() {
        return Optional.ofNullable(time);
    }

    public Optional<URI> getSchemaURL() {
        return Optional.ofNullable(schemaURL);
    }

    public Optional<String> getContentType() {
        return Optional.ofNullable(contentType);
    }

    public Optional<T> getData() {
        return Optional.ofNullable(data);
    }

    @JsonIgnore
    public Optional<Map<String, JsonNode>> getExtensionsAsMap() {
        return Optional.ofNullable(extensionsMap);
    }

    /**
     * Method is needed to comply with cloud event parent class.
     * Use getExtensionsAsMap!
     */
    @Deprecated
    @JsonIgnore
    public Optional<List<Extension>> getExtensions() {
        List<Extension> extensionList = new LinkedList<>();
        this.extensionsMap.forEach((key, value) -> {
            extensionList.add(new UnknownExtension(key, value));
        });
        return Optional.ofNullable(extensionList);
    }

    public Optional<String> getCorrelationId() {
        return Optional.ofNullable(correlationId);
    }

    public Optional<String> getCreatedFrom() {
        return Optional.ofNullable(createdFrom);
    }

    public Optional<List<RouteHistory>> getRoute() {
        return Optional.ofNullable(route);
    }

    public Optional<LinkedList<List<String>>> getRoutingSlip() {
        return Optional.ofNullable(routingSlip);
    }

    public Optional<Boolean> isTestMessage() {
        return Optional.ofNullable(isTestMessage);
    }

    public Optional<String> getFilterOutBeforeTopic() {
        return Optional.ofNullable(filterOutBeforeTopic);
    }

    public Optional<Boolean> isErrorMessage() {
        return Optional.ofNullable(isErrorMessage);
    }

    public Optional<String> getErrorMessage() {
        return Optional.ofNullable(errorMessage);
    }

    public Optional<String> getErrorTrace() {
        return Optional.ofNullable(errorTrace);
    }

    public Optional<ZonedDateTime> getExpiryDate() {
        return Optional.ofNullable(expiryDate);
    }

    public Optional<String> getSequenceId() {
        return Optional.ofNullable(sequenceId);
    }

    public Optional<Integer> getSequenceNumber() {
        return Optional.ofNullable(sequenceNumber);
    }

    public Optional<Integer> getSequenceSize() {
        return Optional.ofNullable(sequenceSize);
    }

    public Optional<String> getReturnTopic() {
        return Optional.ofNullable(returnTopic);
    }

    public Optional<String> getDataRef() {
        return Optional.ofNullable(dataRef);
    }

    public Optional<String> getSubject() {
        return Optional.ofNullable(subject);
    }
}
