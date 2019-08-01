package rmq.summit.reactive.lesson1;

import java.lang.reflect.Executable;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.ResourcesSpecification;
import reactor.rabbitmq.Sender;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import static reactor.rabbitmq.RabbitFlux.createSender;

@SpringBootApplication
public class Lesson1Application {

	public static void main(String[] args) {
		SpringApplication.run(Lesson1Application.class, args);
	}

	@Bean
	public Mono<Connection> connection() {
		ConnectionFactory factory = new ConnectionFactory();
		return Mono
				.fromCallable(() -> factory.newConnection())
				.doOnNext(c -> System.out.println("Connection established: " + c))
				.cache();
	}

	@Bean
	CommandLineRunner producer() {
		return args -> {
			System.out.println("Sending messages");

			Flux<Integer> integers = Flux.range(1, 100);

			Flux<OutboundMessage> messages = integers
					.doOnNext(i -> System.out.printf("Sending : %d\n", i))
					.map(i ->
							new OutboundMessage("", "lesson1", String.valueOf(i).getBytes()));

			Sender sender = RabbitFlux.createSender();

			//Option 1

//			Mono<AMQP.Queue.DeclareOk> untilLesson1Declared = sender
//					.declare(ResourcesSpecification
//							.queue("lesson1").durable(true));
//			Mono<Void> sendMessages = sender.send(messages).delaySubscription(untilLesson1Declared);
//			sendMessages.subscribe();

			// Option 2

			// Once lesson1 queue is created then send messages
			Mono<AMQP.Queue.DeclareOk> onceLesson1Declared = sender
					.declare(ResourcesSpecification
							.queue("lesson1").durable(true));

			onceLesson1Declared
					.then(sender.send(messages))
					.subscribe();

			Thread.sleep(5000);
			sender.close();
		};
	}
}


