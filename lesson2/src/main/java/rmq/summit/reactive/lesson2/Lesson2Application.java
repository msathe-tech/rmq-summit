package rmq.summit.reactive.lesson2;

import java.time.Duration;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ResourcesSpecification;
import reactor.rabbitmq.Sender;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class Lesson2Application {

	public static void main(String[] args) {
		SpringApplication.run(Lesson2Application.class, args);
	}

	static String QUEUE_NAME="lesson2";

	@Bean
	CommandLineRunner producer() {
		return args -> {
			// consumer
			System.out.println("Consumer started");
			Receiver receiver = RabbitFlux.createReceiver();
			Flux<Delivery> deliveryFlux = receiver.consumeAutoAck(QUEUE_NAME);

			deliveryFlux.subscribe(m ->
					System.out.printf("Received message: %s\n", new String(m.getBody())));

			// producer
			System.out.println("Producer started");

			Flux<Integer> integers = Flux.range(1, 50);
			Flux<OutboundMessage> messages = integers
					.doOnNext(i -> System.out.printf("Adding: %d to queue\n", i))
					.delayElements(Duration.ofMillis(100))
					.map(i ->
							new OutboundMessage("", QUEUE_NAME, String.valueOf(i).getBytes()));
			
			Sender sender = RabbitFlux.createSender();

			Mono<AMQP.Queue.DeclareOk> onceLesson2Declared = sender
					.declare(ResourcesSpecification
							.queue("lesson2").durable(true));

			int i = 1;
			onceLesson2Declared
					.then(sender
							.send(messages)
							//.delayElement(Duration.ofMillis(100))
							//.doOnNext(o -> System.out.printf("Sending message: ", o))
							)
					.subscribe(System.out::println);

			Thread.sleep(15000);
			sender.close();
			receiver.close();
		};

	}
}
