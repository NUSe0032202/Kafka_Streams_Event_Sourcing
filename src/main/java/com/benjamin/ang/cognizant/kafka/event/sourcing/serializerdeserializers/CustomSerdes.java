package com.benjamin.ang.cognizant.kafka.event.sourcing.serializerdeserializers;

import com.benjamin.ang.cognizant.kafka.event.sourcing.aggregates.CustomerAggregate;
import com.benjamin.ang.cognizant.kafka.event.sourcing.command.CommandWrapper;
import com.benjamin.ang.cognizant.kafka.event.sourcing.command.CustomerCreateCommand;
import com.benjamin.ang.cognizant.kafka.event.sourcing.command.CustomerUpdateCommand;
import com.benjamin.ang.cognizant.kafka.event.sourcing.events.CustomerCreatedEvent;
import com.benjamin.ang.cognizant.kafka.event.sourcing.events.CustomerUpdatedEvent;
import com.benjamin.ang.cognizant.kafka.event.sourcing.events.Event;
import com.benjamin.ang.cognizant.kafka.event.sourcing.events.EventWrapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.ArrayList;
import java.util.List;

public class CustomSerdes {

    static public final class EventSerde extends Serdes.WrapperSerde<CustomerCreatedEvent> {
        public EventSerde() {
            super(new JsonSerializer<>(CustomerCreatedEvent.class)
                    ,new JsonDeserializer<>(CustomerCreatedEvent.class));
        }
    }

    static public final class AggregateSerde extends Serdes.WrapperSerde<CustomerAggregate> {
        public AggregateSerde() {
            super(new JsonSerializer<>(CustomerAggregate.class)
                    ,new JsonDeserializer<>(CustomerAggregate.class));
        }
    }

    static public final class CommandSerde extends Serdes.WrapperSerde<CustomerCreateCommand> {
        public CommandSerde() {
            super(new JsonSerializer<>(CustomerCreateCommand.class)
                    ,new JsonDeserializer<>(CustomerCreateCommand.class));
        }
    }

    static public final class UpdateEventsSerde extends Serdes.WrapperSerde<CustomerUpdatedEvent> {
        public UpdateEventsSerde() {
            super(new JsonSerializer<>(CustomerUpdatedEvent.class)
                    ,new JsonDeserializer<>(CustomerUpdatedEvent.class));
        }
    }

    static public final class EventsSerde extends Serdes.WrapperSerde<Event> {
        public EventsSerde() {
            super(new JsonSerializer<>(Event.class)
                    ,new JsonDeserializer<>(Event.class));
        }
    }

    static public final class UpdateCommandSerde extends Serdes.WrapperSerde<CustomerUpdateCommand> {
        public UpdateCommandSerde() {
            super(new JsonSerializer<>(CustomerUpdateCommand.class)
                    ,new JsonDeserializer<>(CustomerUpdateCommand.class));
        }
    }
    //Event Wrapper Serializer/Deserializer
    static public final class EventWrapperSerde extends Serdes.WrapperSerde<EventWrapper> {
        public EventWrapperSerde() {
            super(new JsonSerializer<>(EventWrapper.class)
                    ,new JsonDeserializer<>(EventWrapper.class));
        }
    }

    static public final class CommandWrapperSerde extends Serdes.WrapperSerde<CommandWrapper> {
        public CommandWrapperSerde() {
            super(new JsonSerializer<>(CommandWrapper.class)
                    ,new JsonDeserializer<>(CommandWrapper.class));
        }
    }

    public static Serde<CommandWrapper> CommandWrapper() {return new CustomSerdes.CommandWrapperSerde();}

    public static Serde<EventWrapper> EventWrapper() {return  new CustomSerdes.EventWrapperSerde();}

    public static Serde<CustomerUpdateCommand> CustomerUpdateCommand() {
        return new CustomSerdes.UpdateCommandSerde();
    }

    public static Serde<Event> Events() {
        return new CustomSerdes.EventsSerde();
    }

    public static Serde<CustomerUpdatedEvent> CustomerUpdate() {
        return new CustomSerdes.UpdateEventsSerde();
    }

    public static Serde<CustomerCreatedEvent> Event () {
        return new CustomSerdes.EventSerde();
    }

    public static Serde<CustomerAggregate> Aggregate () {
        return new CustomSerdes.AggregateSerde();
    }

    public static Serde<CustomerCreateCommand> Command() {
        return new CustomSerdes.CommandSerde();
    }
}
