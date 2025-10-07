package com.epecuen.notificationservice.consumers;

import com.epecuen.commons.models.HighMsg;
import com.epecuen.notificationservice.service.contracts.MailSender;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer for user-related messages.
 * This class listens to the "userTopic" Kafka topic and processes incoming HighMsg messages.
 * It is part of the notification service.
 *
 * @author caito
 *
 */
@Component
@RequiredArgsConstructor
public class UserConsumer {
    private final MailSender mailSender;

    /**
     * Listens to the "userTopic" Kafka topic and processes HighMsg messages.
     *
     * @param msg the HighMsg message received from the Kafka topic
     */
    @KafkaListener(topics = "userTopic", groupId = "user-service-group")
    public void HandleRegister(HighMsg msg){
        System.out.println("Recived message : " + msg.getEmail() + " " + msg.getUsername() + " "
                + msg.getValidationToken());
        Map<String, String> data = new HashMap<>();
        data.put("name", msg.getUsername());
        data.put("token", msg.getValidationToken());
        mailSender.sendEmailWithTemplate(new String[]{msg.getEmail()},
                "Account Activation - No Reply",
                "templates/account-activation.html",
                data);
    }
}
