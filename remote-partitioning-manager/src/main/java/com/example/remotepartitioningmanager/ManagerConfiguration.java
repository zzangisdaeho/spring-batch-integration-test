/*
 * Copyright 2018-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.remotepartitioningmanager;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.kafka.inbound.KafkaMessageSource;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConsumerProperties;
import org.springframework.messaging.MessageHandler;

import java.util.HashMap;

/**
 * This configuration class is for the manager side of the remote partitioning sample. The
 * manager step will create 3 partitions for workers to process.
 *
 * @author Mahmoud Ben Hassine
 */
@Configuration
@EnableBatchProcessing
@EnableBatchIntegration
public class ManagerConfiguration {

	@Value("${broker.url}")
	private String brokerUrl;

	@Value("${group.id}")
	private String groupId;


	private static final int GRID_SIZE = 3;

	private final JobBuilderFactory jobBuilderFactory;

	private final RemotePartitioningManagerStepBuilderFactory managerStepBuilderFactory;

	public ManagerConfiguration(JobBuilderFactory jobBuilderFactory,
                                RemotePartitioningManagerStepBuilderFactory managerStepBuilderFactory) {

		this.jobBuilderFactory = jobBuilderFactory;
		this.managerStepBuilderFactory = managerStepBuilderFactory;
	}

	@Bean
	public ConsumerFactory<String, String> connectionConsumerFactory() {
		HashMap<String, Object> configs = new HashMap<>();
		configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerUrl);
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		return new DefaultKafkaConsumerFactory<>(configs);
	}

	@Bean
	public ProducerFactory<String, String> connectionProducerFactory() {
		HashMap<String, Object> configs = new HashMap<>();
		configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerUrl);
		configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		return new DefaultKafkaProducerFactory<>(configs);
	}


	/*
	 * Configure outbound flow (requests going to workers)
	 */
	@Bean
	public DirectChannel requests() {
		return new DirectChannel();
	}

//	@Bean
//	public IntegrationFlow outboundFlow(ActiveMQConnectionFactory connectionFactory) {
//		return IntegrationFlow.from(requests()).handle(Jms.outboundAdapter(connectionFactory).destination("requests"))
//				.get();
//	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(connectionProducerFactory());
	}

	@Bean
	@ServiceActivator(inputChannel = "requests")
	public MessageHandler handler() {
		KafkaProducerMessageHandler<String, String> handler =
				new KafkaProducerMessageHandler<>(kafkaTemplate());
		handler.setTopicExpression(new LiteralExpression("remote-partitioning-requests"));
//        handler.setMessageKeyExpression(new LiteralExpression("someKey"));
//        handler.setSendSuccessChannel()
//        handler.setSendFailureChannel();
		return handler;
	}

	/*
	 * Configure inbound flow (replies coming from workers)
	 */
	@Bean
	public DirectChannel replies() {
		return new DirectChannel();
	}

//	@Bean
//	public IntegrationFlow inboundFlow(ActiveMQConnectionFactory connectionFactory) {
//		return IntegrationFlow.from(Jms.messageDrivenChannelAdapter(connectionFactory).destination("replies"))
//				.channel(replies()).get();
//	}

	@InboundChannelAdapter(channel = "replies", poller = @Poller(fixedDelay = "5000"))
	@Bean
	public KafkaMessageSource<String, String> source(ConsumerFactory<String, String> cf)  {
		return new KafkaMessageSource<>(cf, new ConsumerProperties("remote-partitioning-replies"));
	}

	/*
	 * Configure the manager step
	 */
	@Bean
	public Step managerStep() {
		return this.managerStepBuilderFactory.get("managerStep").partitioner("workerStep", new BasicPartitioner())
				.gridSize(GRID_SIZE).outputChannel(requests()).inputChannel(replies()).build();
	}

	@Bean
	public Job remotePartitioningJob() {
		return this.jobBuilderFactory.get("remotePartitioningJob").start(managerStep()).build();
	}

}
