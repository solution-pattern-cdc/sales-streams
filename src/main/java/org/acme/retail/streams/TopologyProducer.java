package org.acme.retail.streams;

import java.util.Collections;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import io.debezium.serde.DebeziumSerdes;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import org.acme.retail.streams.model.AggregatedSale;
import org.acme.retail.streams.model.LineItem;
import org.acme.retail.streams.model.Sale;
import org.acme.retail.streams.model.SaleAndLineItem;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class TopologyProducer {

    private static final Logger log = LoggerFactory.getLogger(TopologyProducer.class);

    @ConfigProperty(name = "topic.sale-change-event")
    String saleChangeEventTopic;

    @ConfigProperty(name = "topic.lineitem-change-event")
    String lineItemChangeEventTopic;

    @ConfigProperty(name = "topic.aggregated-sale")
    String aggregatedSaleTopic;

    @Produces
    public Topology buildTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        final Serde<Long> saleKeySerde = DebeziumSerdes.payloadJson(Long.class);
        saleKeySerde.configure(Collections.emptyMap(), true);
        final Serde<Sale> saleSerde = DebeziumSerdes.payloadJson(Sale.class);
        saleSerde.configure(Collections.singletonMap("from.field", "after"), false);

        final Serde<Long> lineItemKeySerde = DebeziumSerdes.payloadJson(Long.class);
        lineItemKeySerde.configure(Collections.emptyMap(), true);
        final Serde<LineItem> lineItemSerde = DebeziumSerdes.payloadJson(LineItem.class);
        lineItemSerde.configure(Collections.singletonMap("from.field", "after"), false);

        final Serde<AggregatedSale> aggregatedSaleSerde = new ObjectMapperSerde<>(AggregatedSale.class);

        final Serde<SaleAndLineItem> saleAndLineItemSerde = new ObjectMapperSerde<>(SaleAndLineItem.class);

        // KTable of sale events
        KTable<Long, Sale> saleTable = builder.table(saleChangeEventTopic, Consumed.with(saleKeySerde, saleSerde));

        // KTable of lineitem events
        KTable<Long, LineItem> lineItemTable = builder.table(lineItemChangeEventTopic, Consumed.with(lineItemKeySerde, lineItemSerde));

        // Join LineItem events with sale events by foreign key, aggregate Linetem price in sale
        KTable<Long, AggregatedSale> aggregatedSales = lineItemTable
                .join(saleTable, lineItem -> lineItem.sale,
                        (lineItem, sale) -> new SaleAndLineItem(sale, lineItem),
                        Materialized.with(Serdes.Long(), saleAndLineItemSerde))
                .groupBy((key, value) -> KeyValue.pair(value.sale.saleId, value), Grouped.with(Serdes.Long(), saleAndLineItemSerde))
                .aggregate(AggregatedSale::new, (key, value, aggregate) -> aggregate.addLineItem(value),
                        (key, value, aggregate) -> aggregate.removeLineItem(value),
                        Materialized.with(Serdes.Long(), aggregatedSaleSerde));

        aggregatedSales.toStream().to(aggregatedSaleTopic, Produced.with(Serdes.Long(), aggregatedSaleSerde));

        Topology topology = builder.build();
        log.debug(topology.describe().toString());
        return topology;
    }

}
