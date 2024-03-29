package com.pivotal.rabbitmq.importer;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

import javax.annotation.PreDestroy;

/**
 * Spring Cloud Stream
 * ------------------
 * Spring Integration
 * ------------------
 * Spring AMQP                 demo
 * ------------------ ----------------------------
 * Java AMQP library | Java Reactive AMPQ library |
 *
 */
@Configuration
public class RabbitMQConfiguration {

    @Bean
    Mono<Connection> connection(RabbitProperties rabbitProperties) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(rabbitProperties.getHost());
        connectionFactory.setPort(rabbitProperties.getPort());
        connectionFactory.setUsername(rabbitProperties.getUsername());
        connectionFactory.setPassword(rabbitProperties.getPassword());

        return Mono.fromCallable(() -> connectionFactory.newConnection()).cache();
    }

    @Bean
    Sender sender(Mono<Connection> connectionMono) {
        return RabbitFlux.createSender(new SenderOptions().connectionMono(connectionMono));
    }

    @Autowired
    private Mono<Connection> connection;

    @PreDestroy
    public void close() throws Exception {
        connection.block().close();
    }

}
