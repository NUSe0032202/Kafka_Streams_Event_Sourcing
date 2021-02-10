package com.benjamin.ang.cognizant.kafka.event.sourcing;

import com.benjamin.ang.cognizant.kafka.event.sourcing.aggregates.CustomerAggregate;
import com.benjamin.ang.cognizant.kafka.event.sourcing.command.CustomerCreateCommand;
import com.benjamin.ang.cognizant.kafka.event.sourcing.events.CustomerCreatedEvent;
import com.benjamin.ang.cognizant.kafka.event.sourcing.serializerdeserializers.CustomSerdes;
import org.apache.kafka.streams.state.*;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

//@SpringBootApplication
//@Controller
public class Application {

	KeyValueStore<UUID,CustomerCreatedEvent> eventStore = null;
	KeyValueStore<UUID, CustomerAggregate> aggregateStore = null;

	@Autowired
	private InteractiveQueryService queryService;

	@PostMapping("/create")
	public void createCustomer (@RequestBody CustomerCreateCommand command) {
		Properties producerProp = new Properties();
		producerProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		Producer<UUID,CustomerCreateCommand> producerCustomer = new KafkaProducer<>(producerProp
				,Serdes.UUID().serializer()
				,CustomSerdes.Command().serializer());
		ProducerRecord<UUID,CustomerCreateCommand> createCommand = new ProducerRecord<>("test-new-approach-command-topic-5"
				,UUID.randomUUID(), command);
		producerCustomer.send(createCommand);
	}


	@PostMapping("/getEvents")
	public ResponseEntity<ArrayList<String>> getEvents() {
		System.out.println("Retrieving events");
		ArrayList<String> list = new ArrayList<>();
		KeyValueIterator<UUID,CustomerCreatedEvent> keyValueIterator = queryService
				.getQueryableStore("test-new-approach-event-store-5", QueryableStoreTypes.<UUID,
						CustomerCreatedEvent>keyValueStore()).all();
		while(keyValueIterator.hasNext()) {
			UUID key = keyValueIterator.peekNextKey();
			CustomerCreatedEvent event = keyValueIterator.next().value;
			list.add(key.toString()+ " " +event.toString());
			//list.add(keyValueIterator.next().key+" " +event.toString());
		}
		return new ResponseEntity<ArrayList<String>>(list, HttpStatus.OK);
	}

	@PostMapping("/getAggregates")
	public ResponseEntity<ArrayList<String>> getAggregates() {
		System.out.println("Retreieving aggregates");
		ArrayList<String> list = new ArrayList<>();
		KeyValueIterator<UUID,CustomerAggregate> keyValueIterator = queryService
				.getQueryableStore("test-new-approach-aggregate-store-5", QueryableStoreTypes.<UUID,
						CustomerAggregate>keyValueStore()).all();
		while(keyValueIterator.hasNext()) {
			UUID key = keyValueIterator.peekNextKey();
			CustomerAggregate event = keyValueIterator.next().value;
			list.add(key.toString()+ " " +event.toString());
			//list.add(keyValueIterator.next().key+" " +event.toString());
		}
		return new ResponseEntity<ArrayList<String>>(list, HttpStatus.OK);
	}

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public StoreBuilder eventStore() {
		return Stores.keyValueStoreBuilder(
				Stores.persistentKeyValueStore("test-new-approach-event-store-5"), Serdes.UUID(),
				CustomSerdes.Event()
		);
	}

	@Bean
	public StoreBuilder aggregateStore() {
		return Stores.keyValueStoreBuilder(
				Stores.persistentKeyValueStore("test-new-approach-aggregate-store-5"), Serdes.UUID(),
				CustomSerdes.Aggregate()
		);
	}

	@Bean
	public Consumer<KStream<UUID, CustomerCreateCommand>> commandProcessor () {
		return input -> input.process((ProcessorSupplier<UUID,CustomerCreateCommand>) () -> new Processor<UUID,
				CustomerCreateCommand>() {

			@Override
			public void init(ProcessorContext processorContext) {
				System.out.println("Commandprocessor init");
				eventStore = (KeyValueStore<UUID, CustomerCreatedEvent>) processorContext
						.getStateStore("test-new-approach-event-store-5");
			}

			@Override
			public void process(UUID uuid, CustomerCreateCommand command) {
				CustomerCreatedEvent event = new CustomerCreatedEvent(command);
				System.out.println("Attemping to save event to store");
				eventStore.put(uuid,event);
				Properties producerProp = new Properties();
				producerProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
				Producer<UUID, CustomerCreatedEvent> producerCreatedEvent = new KafkaProducer<>(producerProp
						,Serdes.UUID().serializer()
						,CustomSerdes.Event().serializer());
				ProducerRecord<UUID,CustomerCreatedEvent> customerCreatedEvent = new ProducerRecord<>(
						"test-new-approach-event-topic-5"
						,uuid, event);
				producerCreatedEvent.send(customerCreatedEvent);
			}

			@Override
			public void close() {

			}
		},"test-new-approach-event-store-5");
	}

	@Bean
	public Consumer<KStream<UUID,CustomerCreatedEvent>> eventProcessor () {
		return input -> input.process((ProcessorSupplier<UUID,CustomerCreatedEvent>) () -> new Processor<UUID, CustomerCreatedEvent>() {

			@Override
			public void init(ProcessorContext processorContext) {
				System.out.println("eventProcessor init");
				aggregateStore = (KeyValueStore<UUID, CustomerAggregate>) processorContext
						.getStateStore("test-new-approach-aggregate-store-5");
			}

			@Override
			public void process(UUID uuid, CustomerCreatedEvent event) {
				CustomerAggregate aggregate = new CustomerAggregate(event);
				System.out.println("Attempting to save aggregate to DB");
				aggregateStore.put(uuid,aggregate);
			}

			@Override
			public void close() {

			}
		},"test-new-approach-aggregate-store-5");
	}
}
