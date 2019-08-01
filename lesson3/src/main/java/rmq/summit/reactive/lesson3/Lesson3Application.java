package rmq.summit.reactive.lesson3;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


@SpringBootApplication
public class Lesson3Application {

	// This lab assumes QUEUE already exists
	static final String QUEUE = "lesson2";
	private static final Logger LOGGER = LoggerFactory.getLogger(Lesson3Application.class);
	@Autowired
	Mono<Connection> connectionMono;
//	@Autowired
//	AmqpAdmin amqpAdmin;

	public static void main(String[] args) {
		SpringApplication.run(Lesson3Application.class, args).close();
	}

	// the mono for connection, it is cached to re-use the connection across sender and receiver instances
	// this should work properly in most cases
	@Bean()
	Mono<Connection> connectionMono() {
		ConnectionFactory factory = new ConnectionFactory();
		return Mono
				.fromCallable(() -> factory.newConnection())
				.doOnNext(c -> System.out.printf("Connection established: %s\n", c))
				.cache();
	}

	@Bean
	Sender sender(Mono<Connection> connectionMono) {
		return RabbitFlux.createSender(new SenderOptions().connectionMono(connectionMono));
	}

	@Bean
	Receiver receiver(Mono<Connection> connectionMono) {
		return RabbitFlux.createReceiver(new ReceiverOptions().connectionMono(connectionMono));
	}

	@Bean
	Flux<Delivery> deliveryFlux(Receiver receiver) {
		return receiver.consumeNoAck(QUEUE);
	}

//	@PostConstruct
//	public void init() {
//		amqpAdmin.declareQueue(new Queue(QUEUE, false, false, true));
//	}

	@PreDestroy
	public void close() throws Exception {
		connectionMono.block().close();
	}

	// a runner that publishes messages with the sender bean and consumes them with the receiver bean
	@Component
	static class Runner implements CommandLineRunner {

		final Sender sender;
		final Flux<Delivery> deliveryFlux;
		final AtomicBoolean latchCompleted = new AtomicBoolean(false);

		Runner(Sender sender, Flux<Delivery> deliveryFlux) {
			this.sender = sender;
			this.deliveryFlux = deliveryFlux;
		}

		@Override
		public void run(String... args) throws Exception {
			int messageCount = 10;
			CountDownLatch latch = new CountDownLatch(messageCount);
			deliveryFlux.subscribe(m -> {
				LOGGER.info("Received message {}", new String(m.getBody()));
				latch.countDown();
			});
			LOGGER.info("Sending messages...");
			sender.send(Flux
							.range(1, messageCount)
							.map(i -> new OutboundMessage("", QUEUE, ("Message_" + i).getBytes()))
							.doOnNext(m -> LOGGER.info("Sending message " + new String(m.getBody())))
							.delayElements(Duration.ofMillis(500))
					)
					.subscribe();
			latchCompleted.set(latch.await(5, TimeUnit.SECONDS));
		}

	}


}
