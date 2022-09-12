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
package com.example.remotechunkingmanager;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.RemoteChunkingManagerStepBuilderFactory;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.inbound.KafkaMessageSource;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConsumerProperties;
import org.springframework.messaging.MessageHandler;

import java.util.Arrays;
import java.util.HashMap;

/**
 * This configuration class is for the manager side of the remote chunking sample. The
 * manager step reads numbers from 1 to 6 and sends 2 chunks {1, 2, 3} and {4, 5, 6} to
 * workers for processing and writing.
 *
 * @author Mahmoud Ben Hassine
 */
@Configuration
@EnableBatchProcessing
@EnableBatchIntegration
@EnableIntegration
public class ManagerConfig {

    @Value("${broker.url}")
    private String brokerUrl;

    @Value("${group.id}")
    private String groupId;

    private final JobBuilderFactory jobBuilderFactory;

    private final RemoteChunkingManagerStepBuilderFactory managerStepBuilderFactory;

    public ManagerConfig(JobBuilderFactory jobBuilderFactory, RemoteChunkingManagerStepBuilderFactory managerStepBuilderFactory) {
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

//    @Bean
//    public IntegrationFlow inboundFlow(ActiveMQConnectionFactory connectionFactory) {
//        return IntegrationFlow.from(Jms.messageDrivenChannelAdapter(connectionFactory).destination("requests"))
//                .channel(requests()).get();
//    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(connectionProducerFactory());
    }

    @Bean
    @ServiceActivator(inputChannel = "requests")
    public MessageHandler handler() {
        KafkaProducerMessageHandler<String, String> handler =
                new KafkaProducerMessageHandler<>(kafkaTemplate());
        handler.setTopicExpression(new LiteralExpression("remote-chunking-requests"));
//        handler.setMessageKeyExpression(new LiteralExpression("someKey"));
//        handler.setSendSuccessChannel()
//        handler.setSendFailureChannel();
        return handler;
    }

    /*
     * Configure inbound flow (replies coming from workers)
     */

//    @Bean
//    public IntegrationFlow outboundFlow(ActiveMQConnectionFactory connectionFactory) {
//        return IntegrationFlow.from(replies()).handle(Jms.outboundAdapter(connectionFactory).destination("replies"))
//                .get();
//    }

    @Bean
    public QueueChannel replies() {
        return new QueueChannel();
    }

    @InboundChannelAdapter(channel = "replies", poller = @Poller(fixedDelay = "5000"))
    @Bean
    public KafkaMessageSource<String, String> source(ConsumerFactory<String, String> cf)  {
        return new KafkaMessageSource<>(cf, new ConsumerProperties("remote-chunking-replies"));
    }


    /*
     * Configure manager step components
     */
    @Bean
    public ListItemReader<Integer> itemReader() {
        return new ListItemReader<>(Arrays.asList(1, 2, 3, 4, 5, 6));
    }

    @Bean
    public TaskletStep managerStep() {
        return this.managerStepBuilderFactory.get("managerStep").<Integer, Integer>chunk(3).reader(itemReader())
                .outputChannel(requests()).inputChannel(replies()).build();
    }

    @Bean
    public Job remoteChunkingJob() {
        return this.jobBuilderFactory.get("remoteChunkingJob").start(managerStep()).build();
    }

}
