/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.plugin.eventlistener.kafka;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@DefunctConfig("kafka-event-listener.client-config-overrides")
public class KafkaEventListenerConfig
{
    private boolean anonymizationEnabled;
    private boolean publishCreatedEvent = true;
    private boolean publishCompletedEvent = true;
    private boolean publishSplitCompletedEvent;
    private Optional<String> completedTopicName = Optional.empty();
    private Optional<String> createdTopicName = Optional.empty();
    private Optional<String> splitCompletedTopicName = Optional.empty();
    private String brokerEndpoints;
    private Optional<String> clientId = Optional.empty();
    private DataSize maxRequestSize = DataSize.of(5, MEGABYTE); // Greater than default value because the size of completed events are quite large
    private DataSize batchSize = DataSize.of(16, KILOBYTE); // Default value of batch.size
    private Set<String> excludedFields = Collections.emptySet();
    private Duration requestTimeout = new Duration(10, SECONDS);
    private boolean terminateOnInitializationFailure = true;
    private Optional<String> environmentVariablePrefix = Optional.empty();
    private List<File> resourceConfigFiles = ImmutableList.of();

    public boolean isAnonymizationEnabled()
    {
        return anonymizationEnabled;
    }

    @Config("kafka-event-listener.anonymization.enabled")
    public KafkaEventListenerConfig setAnonymizationEnabled(boolean anonymizationEnabled)
    {
        this.anonymizationEnabled = anonymizationEnabled;
        return this;
    }

    @NotEmpty
    public String getBrokerEndpoints()
    {
        return brokerEndpoints;
    }

    @Config("kafka-event-listener.broker-endpoints")
    public KafkaEventListenerConfig setBrokerEndpoints(String brokerEndpoints)
    {
        this.brokerEndpoints = brokerEndpoints;
        return this;
    }

    public Optional<String> getClientId()
    {
        return clientId;
    }

    @Config("kafka-event-listener.client-id")
    public KafkaEventListenerConfig setClientId(String clientId)
    {
        this.clientId = Optional.ofNullable(clientId);
        return this;
    }

    public DataSize getMaxRequestSize()
    {
        return maxRequestSize;
    }

    @ConfigDescription("The maximum size of a request/message in bytes")
    @Config("kafka-event-listener.max-request-size")
    public KafkaEventListenerConfig setMaxRequestSize(DataSize maxRequestSize)
    {
        this.maxRequestSize = maxRequestSize;
        return this;
    }

    public DataSize getBatchSize()
    {
        return batchSize;
    }

    @ConfigDescription("Value that specifies the size to batch before sending records to Kafka")
    @Config("kafka-event-listener.batch-size")
    public KafkaEventListenerConfig setBatchSize(DataSize batchSize)
    {
        this.batchSize = batchSize;
        return this;
    }

    public Optional<String> getCompletedTopicName()
    {
        return completedTopicName;
    }

    @Config("kafka-event-listener.completed-event.topic")
    public KafkaEventListenerConfig setCompletedTopicName(String completedTopicName)
    {
        this.completedTopicName = Optional.ofNullable(completedTopicName);
        return this;
    }

    public Optional<String> getCreatedTopicName()
    {
        return createdTopicName;
    }

    @Config("kafka-event-listener.created-event.topic")
    public KafkaEventListenerConfig setCreatedTopicName(String createdTopicName)
    {
        this.createdTopicName = Optional.ofNullable(createdTopicName);
        return this;
    }

    public Optional<String> getSplitCompletedTopicName()
    {
        return splitCompletedTopicName;
    }

    @Config("kafka-event-listener.split-completed-event.topic")
    public KafkaEventListenerConfig setSplitCompletedTopicName(String splitCompletedTopicName)
    {
        this.splitCompletedTopicName = Optional.ofNullable(splitCompletedTopicName);
        return this;
    }

    public boolean getPublishCreatedEvent()
    {
        return publishCreatedEvent;
    }

    @ConfigDescription("Whether to publish io.trino.spi.eventlistener.QueryCreatedEvent")
    @Config("kafka-event-listener.publish-created-event")
    public KafkaEventListenerConfig setPublishCreatedEvent(boolean publishCreatedEvent)
    {
        this.publishCreatedEvent = publishCreatedEvent;
        return this;
    }

    public boolean getPublishCompletedEvent()
    {
        return publishCompletedEvent;
    }

    @ConfigDescription("Whether to publish io.trino.spi.eventlistener.QueryCompletedEvent")
    @Config("kafka-event-listener.publish-completed-event")
    public KafkaEventListenerConfig setPublishCompletedEvent(boolean publishCompletedEvent)
    {
        this.publishCompletedEvent = publishCompletedEvent;
        return this;
    }

    public boolean getPublishSplitCompletedEvent()
    {
        return publishSplitCompletedEvent;
    }

    @ConfigDescription("Whether to publish io.trino.spi.eventlistener.SplitCompletedEvent")
    @Config("kafka-event-listener.publish-split-completed-event")
    public KafkaEventListenerConfig setPublishSplitCompletedEvent(boolean publishSplitCompletedEvent)
    {
        this.publishSplitCompletedEvent = publishSplitCompletedEvent;
        return this;
    }

    public Set<String> getExcludedFields()
    {
        return this.excludedFields;
    }

    @ConfigDescription("Comma-separated list of field names to be excluded from the Kafka event (their value will be replaced with null). E.g.: 'payload,user'")
    @Config("kafka-event-listener.excluded-fields")
    public KafkaEventListenerConfig setExcludedFields(Set<String> excludedFields)
    {
        this.excludedFields = requireNonNull(excludedFields, "excludedFields is null").stream()
                .filter(field -> !field.isBlank())
                .collect(toImmutableSet());
        return this;
    }

    @MinDuration("1ms")
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @ConfigDescription("Timeout value to complete a kafka request.")
    @Config("kafka-event-listener.request-timeout")
    public KafkaEventListenerConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public boolean getTerminateOnInitializationFailure()
    {
        return terminateOnInitializationFailure;
    }

    @ConfigDescription("Kafka publisher initialization might fail due to network issues reaching the Kafka brokers. This flag controls whether to throw an exception in such cases.")
    @Config("kafka-event-listener.terminate-on-initialization-failure")
    public KafkaEventListenerConfig setTerminateOnInitializationFailure(boolean terminateOnInitializationFailure)
    {
        this.terminateOnInitializationFailure = terminateOnInitializationFailure;
        return this;
    }

    public Optional<String> getEnvironmentVariablePrefix()
    {
        return environmentVariablePrefix;
    }

    @ConfigDescription("When set, Kafka events will be sent with additional metadata populated from environment variables. " +
            "E.g. if env-var-prefix is set to 'TRINO_INSIGHTS_' and there is an env var TRINO_INSIGHTS_CLUSTER_ID=foo, then Kafka payload metadata will contain CLUSTER_ID=foo.")
    @Config("kafka-event-listener.env-var-prefix")
    public KafkaEventListenerConfig setEnvironmentVariablePrefix(String environmentVariablePrefix)
    {
        this.environmentVariablePrefix = Optional.ofNullable(environmentVariablePrefix);
        return this;
    }

    @NotNull
    public List<@FileExists File> getResourceConfigFiles()
    {
        return resourceConfigFiles;
    }

    @Config("kafka-event-listener.config.resources")
    @ConfigDescription("Optional config files")
    public KafkaEventListenerConfig setResourceConfigFiles(List<String> files)
    {
        this.resourceConfigFiles = files.stream()
                .map(File::new)
                .collect(toImmutableList());
        return this;
    }

    @AssertTrue(message = "Created topic name must be configured when publishing created events is enabled.")
    public boolean isCreatedTopicNamePresent()
    {
        return !publishCreatedEvent || !createdTopicName.orElse("").isBlank();
    }

    @AssertTrue(message = "Completed topic name must be configured when publishing completed events is enabled.")
    public boolean isCompletedTopicNamePresent()
    {
        return !publishCompletedEvent || !completedTopicName.orElse("").isBlank();
    }

    @AssertTrue(message = "Split completed topic name must be configured when publishing split completed events is enabled.")
    public boolean isSplitCompletedTopicNamePresent()
    {
        return !publishSplitCompletedEvent || !splitCompletedTopicName.orElse("").isBlank();
    }
}
