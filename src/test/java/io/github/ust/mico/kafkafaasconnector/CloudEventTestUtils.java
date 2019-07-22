package io.github.ust.mico.kafkafaasconnector;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.ust.mico.kafkafaasconnector.kafka.MicoCloudEventImpl;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class CloudEventTestUtils {

    /**
     * Build a basic cloud event with dummy entries and with the given id.
     *
     * @param id the cloud event id to use
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> basicCloudEvent(String id) {
        try {
            URI uri = new URI("http://example.com/unit-test");
            return new MicoCloudEventImpl<JsonNode>()
                .setId(id)
                .setSource(uri)
                .setType("UnitTestMessage")
                .setTime(ZonedDateTime.now());
        } catch (URISyntaxException e) {
            // Should never happen => no real error handling
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Build a basic cloud event with dummy entries and a random id.
     *
     */
    public static MicoCloudEventImpl<JsonNode> basicCloudEventWithRandomId() {
        return basicCloudEvent("").setRandomId();
    }

    /**
     * Mark the given cloud event as test message.
     *
     * @param message              the cloud event to mark as test message
     * @param filterOutBeforeTopic filter out the message before this topic
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> markAsTestMessage(MicoCloudEventImpl<JsonNode> message, String filterOutBeforeTopic) {
        return message.setIsTestMessage(true).setFilterOutBeforeTopic(filterOutBeforeTopic);
    }

    /**
     * Add a single topic routing step to the message.
     * <p>
     * Create a new routing slip if no present. Appends topic to routing slip.
     *
     * @param message the message to edit
     * @param topic   the topic to add to the routing slip
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> addSingleTopicRoutingStep(MicoCloudEventImpl<JsonNode> message, String topic) {
        ArrayList<String> destinations = new ArrayList<>();
        destinations.add(topic);
        return addMultipleTopicRoutingSteps(message, destinations);
    }

    /**
     * Add multiple topics as a Array to the message
     * <p>
     * Create a new routing slip if none are found. Appends topics to new routing slip
     *
     * @param message
     * @param destinations
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> addMultipleTopicRoutingSteps(MicoCloudEventImpl<JsonNode> message, List<String> destinations) {
        Optional<LinkedList<List<String>>> routingSlip = message.getRoutingSlip();
        LinkedList<List<String>> newRoutingSlip = routingSlip.orElse(new LinkedList<>());
        newRoutingSlip.addLast(destinations);
        return message.setRoutingSlip(newRoutingSlip);
    }

    /**
     * Set a already passed expiry date.
     *
     * @param message the message to edit
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> setPastExpiryDate(MicoCloudEventImpl<JsonNode> message) {
        return message.setExpiryDate(ZonedDateTime.now().minusHours(1));
    }

    /**
     * Set a future date as expiry date.
     *
     * @param message the message to edit
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> setFutureExpiryDate(MicoCloudEventImpl<JsonNode> message) {
        return message.setExpiryDate(ZonedDateTime.now().plusHours(1));
    }

    /**
     * Mark the message as part of a sequence.
     *
     * @param message        the message to mark as part of a sequence
     * @param sequenceId     the sequence id
     * @param sequenceNumber the number this message has in the sequence (1-based index)
     * @param sequenceSize   the total number of messages in the sequence
     * @return
     */
    public static MicoCloudEventImpl<JsonNode> setSequenceAttributes(MicoCloudEventImpl<JsonNode> message, String sequenceId, int sequenceNumber, int sequenceSize) {
        return message.setSequenceId(sequenceId)
            .setSequenceSize(sequenceSize)
            .setSequenceNumber(sequenceNumber);
    }
}
