/*
 * Copyright 2025 Google LLC
 *
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

package com.google.cm.testutils.gcp;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.inject.AbstractModule;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannelBuilder;

/** Guice module providing Pub/Sub emulator bindings. Creates one topic and subscription. */
public final class PubSubEmulatorModule extends AbstractModule {

  private static final CredentialsProvider CREDENTIALS_PROVIDER = NoCredentialsProvider.create();

  private final String projectId;
  private final String topicId;
  private final String subscriptionId;

  /** Creates a new instance. */
  public PubSubEmulatorModule(String projectId, String topicId, String subscriptionId) {
    this.projectId = projectId;
    this.topicId = topicId;
    this.subscriptionId = subscriptionId;
  }

  /** Configures injected dependencies for this module. */
  @Override
  protected void configure() {
    // Provide emulator
    var emulator = new PubSubEmulator(projectId);
    emulator.start();
    bind(PubSubEmulator.class).toInstance(emulator);

    TransportChannelProvider transportChannelProvider =
        FixedTransportChannelProvider.create(
            GrpcTransportChannel.create(
                ManagedChannelBuilder.forTarget(emulator.getHostEndpoint())
                    .usePlaintext()
                    .build()));
    var topicName = TopicName.of(projectId, topicId);
    var subName = ProjectSubscriptionName.of(projectId, subscriptionId);

    try {
      // Create topic
      var topicAdminSettings =
          TopicAdminSettings.newBuilder()
              .setTransportChannelProvider(transportChannelProvider)
              .setCredentialsProvider(CREDENTIALS_PROVIDER)
              .build();
      try (var topicAdminClient = TopicAdminClient.create(topicAdminSettings)) {
        topicAdminClient.createTopic(topicName);
      }

      // Create subscription
      var subAdminSettings =
          SubscriptionAdminSettings.newBuilder()
              .setTransportChannelProvider(transportChannelProvider)
              .setCredentialsProvider(CREDENTIALS_PROVIDER)
              .build();
      try (var subAdminClient = SubscriptionAdminClient.create(subAdminSettings)) {
        subAdminClient.createSubscription(subName, topicName, PushConfig.getDefaultInstance(), 10);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
