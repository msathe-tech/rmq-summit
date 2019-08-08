package rmq.summit.react.lesson5;

import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@SpringBootApplication
public class Lesson5Application {

	public static final String ALPHA_NUMERIC = "^[a-zA-Z0-9]+$";

	public static void main(String[] args) {
		SpringApplication.run(Lesson5Application.class, args);
	}

	@Bean
	CommandLineRunner learnSomething() {
		return args -> {
			Flux<String> fluxOfLines = Flux.just(
					"My name is Madhav",
					"My last name is Sathe",
					"Madhav works at Pivotal",
					"Pivotal sells software",
					"Pivotal Cloud Foundry or PCF it a multi-cloud platform for apps",
					"PCF is the fastest way to production",
					"Madhav loves building apps on PCF",
					"PCF automates dev and ops workflows for cloud native apps",
					"PCF is now moving on K8s",
					"K8s is an OSS container orchestrator that can run on any cloud",
					"Madhav is going to speak at SpringOne Platform",
					"Madhav is going to speak on CI/CD apps on K8s"
				);

			Flux<String> fluxOfWords = fluxOfLines
					.doOnNext(line -> System.out.printf("Line: %s\n", line))
					.flatMap(line -> Flux.fromArray(line.split("\\b")))
					.filter(word -> word.matches(ALPHA_NUMERIC))
					.map(word -> word.toLowerCase())
					.doOnNext(word -> System.out.printf("Word: %s\n", word));

			Flux<GroupedFlux<String, String>> fluxOfGroupedFluxOfWords = fluxOfWords.groupBy(word -> word);

			// Option 1 - not most optimal as it calls subscribe twice
//			fluxOfGroupedFluxOfWords
//					.doOnNext(
//							groupedFluxOfWords -> {
//								System.out.printf("Group key: %s\n", groupedFluxOfWords.key());
//								// Reduce is good when you are limited set of objects to process, e.g. in a batch or window
//								// Reduce is not ideal when you have unbounded set of objects in a continuous stream
//								Mono<Tuple2<String, LongAdder>> wordCountMono = groupedFluxOfWords.reduce(
//										Tuples.of(groupedFluxOfWords.key(), new LongAdder()),
//										(intermediateTupleOfWordCount, word) -> {
//											intermediateTupleOfWordCount.getT2().increment();
//											return intermediateTupleOfWordCount;
//										}
//								);
//								wordCountMono
//										.doOnNext(tuple -> System.out.printf("Word key = %s, Count = %s\n", tuple.getT1(), tuple.getT2()))
//										.subscribe();
//							}
//					)
//					.subscribe();

			// Option 2 - most optimal reactive way
			Flux<Tuple2<String, LongAdder>> fluxOfWordCounters = fluxOfGroupedFluxOfWords
					.flatMap(groupOfFluxOfWords ->
							groupOfFluxOfWords.scan(Tuples.of(groupOfFluxOfWords.key(), new LongAdder()),
									(intermediateTupleOfWordCount, word) -> {
										intermediateTupleOfWordCount.getT2().increment();
										return intermediateTupleOfWordCount;
									})
					)
					.doOnNext(tuple -> System.out.printf("Word key = %s, Count = %s\n", tuple.getT1(), tuple.getT2()))
					;

			fluxOfWordCounters.subscribe();

		};
	}

//	@Bean
//	CommandLineRunner learnFlux() {
//		return args -> {
//
//			Flux<String> fluxOfLines = Flux
//					.just("My name is Madhav",
//							"My last name is Sathe",
//							"Madhav works at Pivotal",
//							"Pivotal sells software",
//							"Pivotal Cloud Foundry or PCF it a multi-cloud platform for apps",
//							"PCF is the fastest way to production",
//							"Madhav loves building apps on PCF",
//							"PCF automates dev and ops workflows for cloud native apps",
//							"PCF is now moving on K8s",
//							"K8s is an OSS container orchestrator that can run on any cloud");
//
//			fluxOfLines
//					.doOnNext(s -> System.out.printf("Line: %s\n", s))
//					.subscribe();
//
//			Flux<String> fluxOfWords = fluxOfLines
//					.flatMap(s -> Flux.fromArray(s.split("\\b")))
//					.filter(w -> w.matches(ALPHA_NUMERIC))
//					.map(w -> w.toLowerCase());
//
//			fluxOfWords
//					.doOnNext(w -> System.out.printf("Word: %s\n", w))
//					.subscribe();
//
//			// The groups need to be drained and consumed downstream for groupBy to work correctly.
//			// Notably when the criteria produces a large amount of groups, it can lead to hanging
//			// if the groups are not suitably consumed downstream (eg. due to a flatMap with a
//			// maxConcurrency parameter that is set too low).
//			Flux<GroupedFlux<String, String>> fluxOfGroupedFluxOfWords = fluxOfWords.groupBy(w -> w);
//
//			fluxOfGroupedFluxOfWords
//					.doOnNext(g -> {
//								System.out.printf("Key: %s\n", g.key());
//								Mono<Tuple2<String, LongAdder>> tupleOfWordCount = g
//								.reduce(Tuples.of(g.key(), new LongAdder()),
//										(intermediateTupleOfWordCount, word) -> {
//											intermediateTupleOfWordCount.getT2().increment();
//											return intermediateTupleOfWordCount;
//										});
//								tupleOfWordCount
//										.doOnNext(t -> System.out.printf("Key = %s, Count = %d\n", t.getT1(), t.getT2().intValue()))
//										.subscribe();
//							})
//					.subscribe();
//
//		};
//	}

}
