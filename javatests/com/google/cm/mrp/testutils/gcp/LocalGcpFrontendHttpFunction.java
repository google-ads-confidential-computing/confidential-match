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

package com.google.cm.mrp.testutils.gcp;

import static com.google.cm.mrp.testutils.gcp.Constants.JOB_VERSION;
import static com.google.cm.mrp.testutils.gcp.Constants.PROJECT_ID;
import static com.google.cm.mrp.testutils.gcp.Constants.PUBSUB_MAX_MESSAGE_SIZE_BYTES;
import static com.google.cm.mrp.testutils.gcp.Constants.PUBSUB_MESSAGE_LEASE_SECONDS;
import static com.google.cm.mrp.testutils.gcp.Constants.PUBSUB_SUBSCRIPTION_ID;
import static com.google.cm.mrp.testutils.gcp.Constants.PUBSUB_TOPIC_ID;
import static com.google.cm.mrp.testutils.gcp.Constants.SPANNER_DATABASE_NAME;
import static com.google.cm.mrp.testutils.gcp.Constants.SPANNER_INSTANCE_NAME;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Converter;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.scp.operator.frontend.service.FrontendService;
import com.google.scp.operator.frontend.service.FrontendServiceImpl;
import com.google.scp.operator.frontend.service.converter.CreateJobRequestWithMetadata;
import com.google.scp.operator.frontend.service.converter.CreateJobRequestWithMetadataToRequestInfoConverter;
import com.google.scp.operator.frontend.service.converter.ErrorCountConverter;
import com.google.scp.operator.frontend.service.converter.ErrorSummaryConverter;
import com.google.scp.operator.frontend.service.converter.GetJobResponseConverter;
import com.google.scp.operator.frontend.service.converter.JobStatusConverter;
import com.google.scp.operator.frontend.service.converter.ResultInfoConverter;
import com.google.scp.operator.frontend.service.gcp.CreateJobRequestHandler;
import com.google.scp.operator.frontend.service.gcp.FrontendServiceHttpFunctionBase;
import com.google.scp.operator.frontend.service.gcp.GetJobByIdRequestHandler;
import com.google.scp.operator.frontend.service.gcp.GetJobRequestHandler;
import com.google.scp.operator.frontend.service.gcp.PutJobRequestHandler;
import com.google.scp.operator.frontend.tasks.gcp.GcpTasksModule;
import com.google.scp.operator.protos.frontend.api.v1.ErrorCountProto;
import com.google.scp.operator.protos.frontend.api.v1.ErrorSummaryProto;
import com.google.scp.operator.protos.frontend.api.v1.GetJobResponseProto;
import com.google.scp.operator.protos.frontend.api.v1.JobStatusProto;
import com.google.scp.operator.protos.frontend.api.v1.ResultInfoProto;
import com.google.scp.operator.protos.shared.backend.ErrorCountProto.ErrorCount;
import com.google.scp.operator.protos.shared.backend.ErrorSummaryProto.ErrorSummary;
import com.google.scp.operator.protos.shared.backend.JobStatusProto.JobStatus;
import com.google.scp.operator.protos.shared.backend.RequestInfoProto.RequestInfo;
import com.google.scp.operator.protos.shared.backend.ResultInfoProto.ResultInfo;
import com.google.scp.operator.protos.shared.backend.metadatadb.JobMetadataProto.JobMetadata;
import com.google.scp.operator.shared.dao.jobqueue.gcp.PubSubJobQueueConfig;
import com.google.scp.operator.shared.dao.jobqueue.gcp.PubSubJobQueueModule;
import com.google.scp.operator.shared.dao.metadatadb.gcp.SpannerJobDb.JobDbSpannerTtlDays;
import com.google.scp.operator.shared.dao.metadatadb.gcp.SpannerJobDb.JobDbTableName;
import com.google.scp.operator.shared.dao.metadatadb.gcp.SpannerJobDbModule;
import com.google.scp.operator.shared.dao.metadatadb.gcp.SpannerMetadataDb.MetadataDbSpannerTtlDays;
import com.google.scp.operator.shared.dao.metadatadb.gcp.SpannerMetadataDbConfig;
import com.google.scp.operator.shared.dao.metadatadb.gcp.SpannerMetadataDbModule;
import com.google.scp.shared.clients.configclient.ParameterClient;
import com.google.scp.shared.clients.configclient.model.GetParameterRequest;
import com.google.scp.shared.mapper.TimeObjectMapper;
import java.util.Optional;

/** Entrypoint for a local frontend that uses GCP clients. */
public final class LocalGcpFrontendHttpFunction extends FrontendServiceHttpFunctionBase {

  /** Constructs a new instance. Required to deploy as a cloud function. */
  public LocalGcpFrontendHttpFunction() {
    this(Guice.createInjector(new LocalGcpFrontendModule()));
  }

  private LocalGcpFrontendHttpFunction(Injector injector) {
    super(
        injector.getInstance(CreateJobRequestHandler.class),
        injector.getInstance(GetJobRequestHandler.class),
        injector.getInstance(PutJobRequestHandler.class),
        injector.getInstance(GetJobByIdRequestHandler.class),
        JOB_VERSION);
  }

  private static final class LocalGcpFrontendModule extends AbstractModule {

    private static final Optional<String> PUBSUB_ENDPOINT =
        Optional.ofNullable(System.getenv("PUBSUB_ENDPOINT"));
    private static final Optional<String> SPANNER_ENDPOINT =
        Optional.ofNullable(System.getenv("SPANNER_ENDPOINT"));
    private static final int JOB_DB_TTL = 1; // days
    private static final String JOB_TABLE_NAME = "JobMetadata";

    @Override
    protected void configure() {
      bind(FrontendService.class).to(FrontendServiceImpl.class);
      bind(ObjectMapper.class).to(TimeObjectMapper.class);
      bind(new TypeLiteral<Converter<JobMetadata, GetJobResponseProto.GetJobResponse>>() {})
          .to(GetJobResponseConverter.class);
      bind(new TypeLiteral<Converter<CreateJobRequestWithMetadata, RequestInfo>>() {})
          .to(CreateJobRequestWithMetadataToRequestInfoConverter.class);
      bind(new TypeLiteral<Converter<ResultInfo, ResultInfoProto.ResultInfo>>() {})
          .to(ResultInfoConverter.class);
      bind(new TypeLiteral<Converter<ErrorSummary, ErrorSummaryProto.ErrorSummary>>() {})
          .to(ErrorSummaryConverter.class);
      bind(new TypeLiteral<Converter<JobStatus, JobStatusProto.JobStatus>>() {})
          .to(JobStatusConverter.class);
      bind(new TypeLiteral<Converter<ErrorCount, ErrorCountProto.ErrorCount>>() {})
          .to(ErrorCountConverter.class);
      bind(String.class).annotatedWith(JobDbTableName.class).toInstance(JOB_TABLE_NAME);
      bind(Integer.class).annotatedWith(MetadataDbSpannerTtlDays.class).toInstance(JOB_DB_TTL);
      bind(Integer.class).annotatedWith(JobDbSpannerTtlDays.class).toInstance(JOB_DB_TTL);
      bind(ParameterClient.class).toInstance(createParameterClient());
      bind(SpannerMetadataDbConfig.class)
          .toInstance(
              SpannerMetadataDbConfig.builder()
                  .setGcpProjectId(PROJECT_ID)
                  .setSpannerInstanceId(SPANNER_INSTANCE_NAME)
                  .setSpannerDbName(SPANNER_DATABASE_NAME)
                  .setEndpointUrl(SPANNER_ENDPOINT)
                  .build());
      bind(PubSubJobQueueConfig.class)
          .toInstance(
              PubSubJobQueueConfig.builder()
                  .setGcpProjectId(PROJECT_ID)
                  .setPubSubTopicId(PUBSUB_TOPIC_ID)
                  .setPubSubSubscriptionId(PUBSUB_SUBSCRIPTION_ID)
                  .setPubSubMaxMessageSizeBytes(PUBSUB_MAX_MESSAGE_SIZE_BYTES)
                  .setPubSubMessageLeaseSeconds(PUBSUB_MESSAGE_LEASE_SECONDS)
                  .setEndpointUrl(PUBSUB_ENDPOINT)
                  .build());
      install(new SpannerMetadataDbModule());
      install(new SpannerJobDbModule());
      install(new PubSubJobQueueModule());
      install(new GcpTasksModule());
    }

    @Singleton
    private ParameterClient createParameterClient() {
      return new ParameterClient() {
        /** Always returns an empty optional. */
        @Override
        public Optional<String> getParameter(String param) {
          return Optional.empty();
        }

        /** Returns an empty optional except MIC_FEATURE_ENABLED and notification_topic_mic flags */
        @Override
        public Optional<String> getParameter(
            String param,
            Optional<String> paramPrefix,
            boolean includeEnvironmentParam,
            boolean getLatest) {
          return Optional.empty();
        }

        /** Always returns an empty optional. */
        @Override
        public Optional<String> getLatestParameter(String param) {
          return Optional.empty();
        }

        /** Always returns an optional of "LOCAL_ARGS". */
        @Override
        public Optional<String> getEnvironmentName() {
          return Optional.of("LOCAL_ARGS");
        }

        @Override
        public Optional<String> getParameter(GetParameterRequest getParameterRequest) {
          return Optional.empty();
        }

        @Override
        public Optional<String> getWorkgroupId() {
          return Optional.empty();
        }
      };
    }
  }
}
