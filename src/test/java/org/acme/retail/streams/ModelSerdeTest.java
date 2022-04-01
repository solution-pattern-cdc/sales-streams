package org.acme.retail.streams;

import java.util.Collections;

import io.debezium.serde.json.JsonSerde;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.acme.retail.streams.model.LineItem;
import org.acme.retail.streams.model.Sale;
import org.acme.retail.streams.model.SaleAndLineItem;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

public class ModelSerdeTest {

    @Test
    public void testDeserializeDebeziumChangeEventSale() {

        String data = "{" +
                "\"before\": null," +
                "\"after\": {" +
                "\"sale_id\": 1006," +
                "\"customer_id\": 1004," +
                "\"date\": 1648721516590000" +
                "}," +
                "\"source\": {" +
                "\"version\": \"1.7.0.Final\"," +
                "\"connector\": \"postgresql\"," +
                "\"name\": \"retail.updates\"," +
                "\"ts_ms\": 1648714316653," +
                "\"snapshot\": \"false\"," +
                "\"db\": \"retail\"," +
                "\"sequence\": \"[\\\"23655184\\\",\\\"23655184\\\"]\"," +
                "\"schema\": \"public\"," +
                "\"table\": \"sale\"," +
                "\"txId\": 549," +
                "\"lsn\": 23655184," +
                "\"xmin\": null" +
                "}," +
                "\"op\": \"c\"," +
                "\"ts_ms\": 1648714316784," +
                "\"transaction\": null" +
                "}";

        JsonSerde<Sale> serde = new JsonSerde<>(Sale.class);
        serde.configure(Collections.singletonMap("from.field", "after"), false);

        Sale sale = serde.deserializer().deserialize("", data.getBytes());
        MatcherAssert.assertThat(sale, Matchers.notNullValue());
        MatcherAssert.assertThat(sale.saleId, Matchers.equalTo(1006L));
        MatcherAssert.assertThat(sale.customer, Matchers.equalTo(1004L));
        MatcherAssert.assertThat(sale.date, Matchers.equalTo(1648721516590000L));
    }

    @Test
    public void testDeserializeDebeziumChangeEventLineItem() {
        String data = "{" +
                 "\"before\": null," +
                 "\"after\": {" +
                 "\"line_item_id\": 1017," +
                 "\"sale_id\": 1007," +
                 "\"product_id\": 1001," +
                 "\"price\": \"12.45\"," +
                 "\"quantity\": 4" +
                 "}," +
                 "\"source\": {" +
                 "\"version\": \"1.7.0.Final\"," +
                 "\"connector\": \"postgresql\"," +
                 "\"name\": \"retail.updates\"," +
                 "\"ts_ms\": 1648714316755," +
                 "\"snapshot\": \"false\"," +
                 "\"db\": \"retail\"," +
                 "\"sequence\": \"[\\\"23656960\\\",\\\"23656960\\\"]\"," +
                 "\"schema\": \"public\"," +
                 "\"table\": \"line_item\"," +
                 "\"txId\": 555," +
                 "\"lsn\": 23656960," +
                 "\"xmin\": null" +
                 "}," +
                 "\"op\": \"c\"," +
                 "\"ts_ms\": 1648714316786," +
                 "\"transaction\": null" +
                 "}";

        JsonSerde<LineItem> serde = new JsonSerde<>(LineItem.class);
        serde.configure(Collections.singletonMap("from.field", "after"), false);

        LineItem lineItem = serde.deserializer().deserialize("", data.getBytes());
        MatcherAssert.assertThat(lineItem, Matchers.notNullValue());
        MatcherAssert.assertThat(lineItem.lineItemId, Matchers.equalTo(1017L));
        MatcherAssert.assertThat(lineItem.sale, Matchers.equalTo(1007L));
        MatcherAssert.assertThat(lineItem.product, Matchers.equalTo(1001L));
        MatcherAssert.assertThat(lineItem.price, Matchers.equalTo("12.45"));
        MatcherAssert.assertThat(lineItem.quantity, Matchers.equalTo(4));
    }

    @Test
    public void testSerdeSaleAndLineItem() {

        Sale sale = new Sale();
        sale.saleId = 1000L;
        sale.customer = 1500L;
        sale.date = System.nanoTime() / 1000;

        LineItem lineItem = new LineItem();
        lineItem.lineItemId = 1100L;
        lineItem.sale = 1000L;
        lineItem.product = 1200L;
        lineItem.price = "25.50";
        lineItem.quantity = 2;

        SaleAndLineItem saleAndLineItem = new SaleAndLineItem(sale, lineItem);

        ObjectMapperSerde<SaleAndLineItem> serde = new ObjectMapperSerde<>(SaleAndLineItem.class);
        byte[] serialized = serde.serializer().serialize("", saleAndLineItem);
        JsonObject json = new JsonObject(Buffer.buffer(serialized));
        MatcherAssert.assertThat(json, Matchers.notNullValue());
        MatcherAssert.assertThat(json.containsKey("sale"), Matchers.is(true));
        MatcherAssert.assertThat(json.containsKey("line_item"), Matchers.is(true));
        MatcherAssert.assertThat(json.getJsonObject("sale").containsKey("sale_id"), Matchers.is(true));
        MatcherAssert.assertThat(json.getJsonObject("sale").getLong("sale_id"), Matchers.equalTo(1000L));
        MatcherAssert.assertThat(json.getJsonObject("line_item").containsKey("line_item_id"), Matchers.is(true));
        MatcherAssert.assertThat(json.getJsonObject("line_item").getLong("line_item_id"), Matchers.equalTo(1100L));

        SaleAndLineItem deserialized = serde.deserializer().deserialize("", serialized);
        MatcherAssert.assertThat(deserialized, Matchers.notNullValue());
        MatcherAssert.assertThat(deserialized.sale, Matchers.notNullValue());
        MatcherAssert.assertThat(deserialized.sale.saleId, Matchers.equalTo(1000L));
        MatcherAssert.assertThat(deserialized.sale.date, Matchers.equalTo(sale.date));
        MatcherAssert.assertThat(deserialized.lineItem, Matchers.notNullValue());
        MatcherAssert.assertThat(deserialized.lineItem.lineItemId, Matchers.equalTo(1100L));
        MatcherAssert.assertThat(deserialized.lineItem.price, Matchers.equalTo("25.50"));
    }



}
