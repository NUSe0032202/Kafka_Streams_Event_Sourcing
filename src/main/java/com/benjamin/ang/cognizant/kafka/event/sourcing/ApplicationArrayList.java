package com.benjamin.ang.cognizant.kafka.event.sourcing;

import com.benjamin.ang.cognizant.kafka.event.sourcing.aggregates.CustomerAggregate;
import com.benjamin.ang.cognizant.kafka.event.sourcing.command.CommandWrapper;
import com.benjamin.ang.cognizant.kafka.event.sourcing.command.CustomerCreateCommand;
import com.benjamin.ang.cognizant.kafka.event.sourcing.command.CustomerUpdateCommand;
import com.benjamin.ang.cognizant.kafka.event.sourcing.events.CustomerCreatedEvent;
import com.benjamin.ang.cognizant.kafka.event.sourcing.events.CustomerUpdatedEvent;
import com.benjamin.ang.cognizant.kafka.event.sourcing.events.EventWrapper;
import com.benjamin.ang.cognizant.kafka.event.sourcing.serializerdeserializers.ArrayListSerde;
import com.benjamin.ang.cognizant.kafka.event.sourcing.serializerdeserializers.CustomSerdes;
import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.*;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
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

@SpringBootApplication
@Controller
public class ApplicationArrayList {

	KeyValueStore<UUID,ArrayList<EventWrapper>> eventStore = null;
	KeyValueStore<UUID, CustomerAggregate> aggregateStore = null;

	@Autowired
	private InteractiveQueryService queryService;

	@PostMapping("/create")
	public void createCustomer (@RequestBody CustomerCreateCommand command) {

		CommandWrapper commandWrapper = new CommandWrapper();
		Gson gson = new Gson();
		commandWrapper.setCommandType("CREATE_COMMAND");
		commandWrapper.setCommandPayload(gson.toJson(command));
		Properties producerProp = new Properties();
		producerProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		Producer<UUID,CommandWrapper> producerCustomer = new KafkaProducer<>(producerProp
					,Serdes.UUID().serializer()
					,CustomSerdes.CommandWrapper().serializer());
		ProducerRecord<UUID,CommandWrapper> createCommand = new ProducerRecord<>("test-arrylist-command-topic-4"
					,UUID.randomUUID(), commandWrapper);
		producerCustomer.send(createCommand);
	}
	@PostMapping("/update")
	public void updateCustomer (@RequestBody CustomerUpdateCommand command) {

		CommandWrapper commandWrapper = new CommandWrapper();
		Gson gson = new Gson();
		commandWrapper.setCommandType("UPDATE_COMMAND");
		commandWrapper.setCommandPayload(gson.toJson(command));
		Properties producerProp = new Properties();
		producerProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		Producer<UUID,CommandWrapper> producerCustomer = new KafkaProducer<>(producerProp
				,Serdes.UUID().serializer()
				,CustomSerdes.CommandWrapper().serializer());
		ProducerRecord<UUID,CommandWrapper> createCommand = new ProducerRecord<>("test-arrylist-command-topic-4"
				,command.getId(), commandWrapper);
		producerCustomer.send(createCommand);
	}

	@PostMapping("/getEvents")
	public ResponseEntity<ArrayList<Pair<UUID, ArrayList<EventWrapper>>>> getEvents() {
		System.out.println("Retrieving events");
		//ArrayList<String> list = new ArrayList<>();
		KeyValueIterator<UUID,ArrayList<EventWrapper>> keyValueIterator = queryService
				.getQueryableStore("test-arrylist-event-store-11", QueryableStoreTypes.<UUID,
						ArrayList<EventWrapper>>keyValueStore()).all();

		ArrayList<Pair<UUID,ArrayList<EventWrapper>>> eventLogs = new ArrayList<>();
		while(keyValueIterator.hasNext()) {
			UUID key = keyValueIterator.peekNextKey();
			ArrayList<EventWrapper> events = keyValueIterator.next().value;
			Pair<UUID, ArrayList<EventWrapper>> pair = new Pair<UUID, ArrayList<EventWrapper>>(key,events);
			eventLogs.add(pair);
		}
		return new ResponseEntity<ArrayList<Pair<UUID, ArrayList<EventWrapper>>>>(eventLogs, HttpStatus.OK);
	}

	@PostMapping("/getAggregates")
	public ResponseEntity<ArrayList<Pair<UUID, CustomerAggregate>>> getAggregates() {
		System.out.println("Retreieving aggregates");
		//ArrayList<String> list = new ArrayList<>();
		KeyValueIterator<UUID,CustomerAggregate> keyValueIterator = queryService
			.getQueryableStore("test-arraylist-aggregate-store-11", QueryableStoreTypes.<UUID,
						CustomerAggregate>keyValueStore()).all();
		ArrayList<Pair<UUID,CustomerAggregate>> aggregateLogs = new ArrayList<>();
		while(keyValueIterator.hasNext()) {
			UUID key = keyValueIterator.peekNextKey();
			CustomerAggregate aggregate = keyValueIterator.next().value;
			Pair<UUID,CustomerAggregate> pair = new Pair<UUID, CustomerAggregate>(key,aggregate);
			aggregateLogs.add(pair);
		}
		return new ResponseEntity<ArrayList<Pair<UUID, CustomerAggregate>>>(aggregateLogs, HttpStatus.OK);
	}

	public static void main(String[] args) {
		SpringApplication.run(ApplicationArrayList.class, args);
	}

	@Bean
	public StoreBuilder eventStoreArrayList() {
		return Stores.keyValueStoreBuilder(
				Stores.persistentKeyValueStore("test-arrylist-event-store-11"), Serdes.UUID(),
				new ArrayListSerde<>(CustomSerdes.EventWrapper())
		);
	}

	@Bean
	public StoreBuilder aggregateStoreArrayList() {
		return Stores.keyValueStoreBuilder(
				Stores.persistentKeyValueStore("test-arraylist-aggregate-store-11"), Serdes.UUID(),
				CustomSerdes.Aggregate()
		);
	}

	@Bean
	public Consumer<KStream<UUID, CommandWrapper>> commandProcessorArrayList () {
		return input -> input.process((ProcessorSupplier<UUID,CommandWrapper>) () -> new Processor<UUID,
				CommandWrapper>() {
			private Gson gson = new Gson();

			@Override
			public void init(ProcessorContext processorContext) {
				System.out.println("Commandprocessor init");
				eventStore = (KeyValueStore<UUID, ArrayList<EventWrapper>>) processorContext
						.getStateStore("test-arrylist-event-store-11");
			}

			@Override
			public void process(UUID uuid, CommandWrapper command) {

				if(command.getCommandType().equals("CREATE_COMMAND")) {
					System.out.println("Processing create command");
					ArrayList<EventWrapper> events = new ArrayList<>();
					//Gson gson = new Gson();

					EventWrapper customerCreatedEventWrapper = new EventWrapper();
					customerCreatedEventWrapper.setEventID(UUID.randomUUID());
					customerCreatedEventWrapper.setEventType("CREATE_CUSTOMER");
					CustomerCreateCommand customerCreatedCommand = gson.fromJson(command.getCommandPayload()
							,CustomerCreateCommand.class);

					//Create the event object
					CustomerCreatedEvent customerCreatedEvent = new CustomerCreatedEvent(customerCreatedCommand);
					customerCreatedEventWrapper.setEventPayload(gson.toJson(customerCreatedEvent));
					///////////

					events.add(customerCreatedEventWrapper);
					eventStore.put(uuid,events);

					Properties producerProp = new Properties();
					producerProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
					Producer<UUID, EventWrapper> createProducer = new KafkaProducer<>(producerProp
								,Serdes.UUID().serializer()
							,CustomSerdes.EventWrapper().serializer());
					ProducerRecord<UUID,EventWrapper> customerCreatedEventProducerRecord = new ProducerRecord<>(
								"test-arrylist-event-topic-4"
							,uuid, customerCreatedEventWrapper);
					createProducer.send(customerCreatedEventProducerRecord);
				}

				if(command.getCommandType().equals("UPDATE_COMMAND")) {
					System.out.println("Processing update command");
					//Gson gson = new Gson();

					EventWrapper customerUpdatedEventWrapper = new EventWrapper();
					customerUpdatedEventWrapper.setEventID(UUID.randomUUID());
					customerUpdatedEventWrapper.setEventType("UPDATE_CUSTOMER");

					CustomerUpdateCommand customerUpdatedCommand = gson.fromJson(command.getCommandPayload()
							,CustomerUpdateCommand.class);

					//Create the event object
					CustomerUpdatedEvent customerUpdatedEvent = new CustomerUpdatedEvent(customerUpdatedCommand);
					customerUpdatedEventWrapper.setEventPayload(gson.toJson(customerUpdatedEvent));
					//////

					//Add event to existing arraylist obtained from store
					ArrayList<EventWrapper> eventArrayList = eventStore.get(uuid);
					eventArrayList.add(customerUpdatedEventWrapper);
					eventStore.put(uuid,eventArrayList);

					Properties producerProp = new Properties();
					producerProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
					Producer<UUID, EventWrapper> updateProducer = new KafkaProducer<>(producerProp
							,Serdes.UUID().serializer()
							,CustomSerdes.EventWrapper().serializer());
					ProducerRecord<UUID,EventWrapper> customerUpdatedEventProducerRecord = new ProducerRecord<>(
							"test-arrylist-event-topic-4"
							,uuid,customerUpdatedEventWrapper);
					updateProducer.send(customerUpdatedEventProducerRecord);
				}
			}
			@Override
			public void close() {

			}
		},"test-arrylist-event-store-11");
	}

	@Bean
	public Consumer<KStream<UUID,EventWrapper>> eventProcessorArrayList () {
		return input -> input.process((ProcessorSupplier<UUID,EventWrapper>) () -> new Processor<UUID, EventWrapper>() {

			@Override
			public void init(ProcessorContext processorContext) {
				System.out.println("eventProcessor init");
				aggregateStore = (KeyValueStore<UUID, CustomerAggregate>) processorContext
						.getStateStore("test-arraylist-aggregate-store-11");
			}

			@Override
			public void process(UUID uuid, EventWrapper event) {

				if(event.getEventType().equals("CREATE_CUSTOMER")) {
					Gson gson = new Gson();
					System.out.println("Attempting to save aggregate to DB");
					CustomerCreatedEvent customerCreatedEvent = gson.fromJson(event.getEventPayload()
							,CustomerCreatedEvent.class);
					CustomerAggregate aggregate = new CustomerAggregate(customerCreatedEvent);
					aggregateStore.put(uuid,aggregate);
				}

				if(event.getEventType().equals("UPDATE_CUSTOMER")) {
					Gson gson = new Gson();
					System.out.println("Attempting to update aggregate");
					CustomerUpdatedEvent customerUpdatedEvent = gson.fromJson(event.getEventPayload()
							,CustomerUpdatedEvent.class);
					CustomerAggregate existingAggregate = aggregateStore.get(uuid);
					existingAggregate.setFirstName(customerUpdatedEvent.getFirstName());
					existingAggregate.setLastName(customerUpdatedEvent.getLastName());
					existingAggregate.setPhoneNumber(customerUpdatedEvent.getPhoneNumber());
					existingAggregate.setEmail(customerUpdatedEvent.getEmail());
					aggregateStore.put(uuid,existingAggregate);
				}
			}
			@Override
			public void close() {

			}
		},"test-arraylist-aggregate-store-11");
	}
}
