# Kafka FaaS Connector

The *Kafka FaaS Connector* is used by [MICO](https://github.com/UST-MICO/mico) to route messages from Kafka in the [CloudEvents](https://github.com/cloudevents/spec) format to an [OpenFaaS](https://github.com/openfaas/faas) function.

## Requirements

*Kafka FaaS Connector* requires Kafka and OpenFaaS.

### Kubernetes

Run Kafka, ZooKeeper and OpenFaaS in your Kubernetes cluster.
You find some notes about Kafka in our [documentation](https://mico-docs.readthedocs.io/en/latest/setup/kubernetes/kafka.html).

Modify `kafka-faas-connector.yml` to set the correct URLs of the OpenFaaS gateway (default: `http://gateway.openfaas:8080`) and the Kafka servers (default: `bootstrap.kafka:9092`).

### Local execution

Launch Kafka and ZooKeeper or establish a connection to already running instances (e.g. via port forwarding).

docker-compose:
```bash
docker-compose up kafka zookeeper
```

Port forwarding:
```bash
kubectl port-forward svc/kafka -n openfaas 9092
kubectl port-forward svc/zookeeper -n openfaas 2181
```

## Usage

Build and push the Docker image:
```bash
docker build -t ustmico/kafka-faas-connector . && docker push ustmico/kafka-faas-connector
```

**Kubernetes:**

Deployment:
```bash
kubectl apply -f kafka-faas-connector.yml
```

**Local execution:**

docker-compose:
```bash
docker-compose up --build kafka-faas-connector
```

## Misc

Actuator get configuration properties:
```bash
kubectl port-forward svc/kafka-faas-connector -n mico-workspace 8080
curl localhost:8080/actuator/configprops | jq . > configmaps.json
```

Get the Kubernetes logs:
```bash
kubectl -n $NAMESPACE logs -f $(kubectl get pods -n $NAMESPACE --selector=run=kafka-faas-connector --output=jsonpath={.items..metadata.name})
```

Restart it by deleting the Kubernetes pod:
```bash
kubectl -n $NAMESPACE delete pod $(kubectl get pods -n $NAMESPACE --selector=run=kafka-faas-connector --output=jsonpath={.items..metadata.name})
```

## Environment Variables
| Variable                          | Description                                                                      | Default Value            |
|-----------------------------------|----------------------------------------------------------------------------------|--------------------------|
| APPLICATION_NAME                  | The spring application name                                                      | kafka-faas-connector     |
| APPLICATION_PORT                  | The server port                                                                  | 8080                     |
| LOGGING_LEVEL_KAFKAFAASCONNECTOR  | Logging level of the Kafka-FaaS-Connector                                        | DEBUG                    |
| LOGGING_LEVEL_KAFKA_CONSUMER_INFO | Logging level of the kafka consumer                                              | INFO                     |
| KAFKA_BOOTSTRAP_SERVERS           | The URLs of the bootstrap servers                                                | localhost:9092           |
| KAFKA_GROUP_ID                    | The group id for kafka                                                           | mico                     |
| KAFKA_TOPIC_INPUT                 | The topic, on which the Kafka-Faas-Connector receives messages                   | transform-request        |
| KAFKA_TOPIC_OUTPUT                | The topic, to which the kafka-faas-connector forwards the processed messages     | transform-result         |
| KAFKA_TOPIC_INVALID_MESSAGE       | The topic, to which invalid messages are forwarded                               | InvalidMessage           |
| KAFKA_TOPIC_DEAD_LETTER           | The topic, to which messages are forwarded that can't/shouldn't be delivered     | DeadLetter               |
| KAFKA_TOPIC_TEST_MESSAGE_OUTPUT   | The topic, to which test messages are forwarded                                  | TestMessagesOutput       |
| OPENFAAS_GATEWAY                  | The URL of the OpenFaaS gateway                                                  | http://127.0.0.1:8080    |
| OPENFAAS_FUNCTION_NAME            | The name of the OpenFaaS function that shall be used for processing the messages | faas-message-transformer |
