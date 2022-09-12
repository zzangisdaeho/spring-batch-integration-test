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
package com.example.remotechunkingworker1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.inbound.KafkaMessageSource;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConsumerProperties;
import org.springframework.messaging.MessageHandler;

import java.util.HashMap;


@Configuration
@EnableBatchProcessing
@EnableBatchIntegration
@EnableIntegration
public class WorkerConfiguration {

	@Value("${broker.url}")
	private String brokerUrl;

	@Value("${group.id}")
	private String groupId;

	private final RemoteChunkingWorkerBuilder<Integer, Integer> remoteChunkingWorkerBuilder;

	public WorkerConfiguration(RemoteChunkingWorkerBuilder<Integer, Integer> remoteChunkingWorkerBuilder) {
		this.remoteChunkingWorkerBuilder = remoteChunkingWorkerBuilder;
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
	 * Configure inbound flow (requests coming from the manager)
	 */
	@Bean
	public DirectChannel requests() {
		return new DirectChannel();
	}


	@InboundChannelAdapter(channel = "requests", poller = @Poller(fixedDelay = "5000"))
	@Bean
	public KafkaMessageSource<String, String> source(ConsumerFactory<String, String> cf)  {
		return new KafkaMessageSource<>(cf, new ConsumerProperties("remote-chunking-requests"));
	}

	/*
	 * Configure outbound flow (replies going to the manager)
	 */
	@Bean
	public DirectChannel replies() {
		return new DirectChannel();
	}


	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(connectionProducerFactory());
	}

	@Bean
	@ServiceActivator(inputChannel = "replies")
	public MessageHandler handler() {
		KafkaProducerMessageHandler<String, String> handler =
				new KafkaProducerMessageHandler<>(kafkaTemplate());
		handler.setTopicExpression(new LiteralExpression("remote-chunking-replies"));
//        handler.setMessageKeyExpression(new LiteralExpression("someKey"));
//        handler.setSendSuccessChannel()
//        handler.setSendFailureChannel();
		return handler;
	}

	/*
	 * Configure worker components
	 */
	@Bean
	public ItemProcessor<Integer, Integer> itemProcessor() {
		return item -> {
			System.out.println("processing item " + item);
			return item;
		};
	}

	@Bean
	public ItemWriter<Integer> itemWriter() {
		return items -> {
			for (Integer item : items) {
				System.out.println("writing item " + item);
			}
		};
	}

	@Bean
	public IntegrationFlow workerIntegrationFlow() {
		return this.remoteChunkingWorkerBuilder.itemProcessor(itemProcessor()).itemWriter(itemWriter())
				.inputChannel(requests()).outputChannel(replies()).build();
	}

}
