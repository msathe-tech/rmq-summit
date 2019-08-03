package rmq.summit.react.lesson4;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ReceiverOptions;
import reactor.rabbitmq.Sender;
import reactor.rabbitmq.SenderOptions;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;

@SpringBootApplication
public class Lesson4Application {

	// Start RMQ on local machine using
	// docker run -d --name rabbitmq -p 15672:15672 -p 5672:5672 rabbitmq:3.7-management
	// Create queues, lines and word-count

	static final String LINES_QUEUE = "lines";
	static final String WORDS_COUNT_QUEUE = "word-count";
	public static final String ALPHANUMERIC = "^[a-zA-Z0-9]+$";


	private static final Logger LOGGER = LoggerFactory.getLogger(Lesson4Application.class);

	public static void main(String[] args) {
		SpringApplication.run(Lesson4Application.class, args);
	}

	@Bean
	Mono<Connection> connectionMono() {
		ConnectionFactory factory = new ConnectionFactory();
		return Mono
				.fromCallable(() -> factory.newConnection())
				.doOnNext(c -> System.out.printf("Connection established: %s", c))
				.cache();
	}



	@EventListener
	public void applicationStarted(ApplicationStartedEvent event) {
		Receiver linesReceiver = RabbitFlux.createReceiver();
		Flux<Delivery> lines = linesReceiver.consumeAutoAck(LINES_QUEUE);
		int linesCount = 10;
		CountDownLatch latch = new CountDownLatch(linesCount);

		Flux<Tuple2<String, LongAdder>> wordsCount = lines.transform(wordCount());

		Sender wordsCountSender = RabbitFlux.createSender();

		wordsCountSender
				.send(wordsCount
						.map(wc -> new OutboundMessage("", WORDS_COUNT_QUEUE,
							String.format("%s:%d\n", wc.getT1(), wc.getT2().longValue()).getBytes())
						)
				)
				.subscribe();

	}

	private Function<Flux<Delivery>, Flux<Tuple2<String, LongAdder>>> wordCount() {
		return f -> f.flatMap(l -> Flux.fromArray(new String(l.getBody()).split("\\b")))
				.filter(w -> w.matches(ALPHANUMERIC))
				.map(String::toLowerCase)
				.groupBy(words -> words)
				.flatMap(emitWordCount());
	}

	private Function<? super GroupedFlux<String, String>, Flux<Tuple2<String, LongAdder>>> emitWordCount() {
		return g -> g.scan(Tuples.of(g.key(), new LongAdder()),
				(t, w) -> {
					t.getT2().increment();
					return t;
				});
	}

}
