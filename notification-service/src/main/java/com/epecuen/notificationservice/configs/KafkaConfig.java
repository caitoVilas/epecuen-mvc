package com.epecuen.notificationservice.configs;

import org.springframework.beans.factory.annotation.Value;

public class KafkaConfig {
    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;


}
