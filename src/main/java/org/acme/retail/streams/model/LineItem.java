package org.acme.retail.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LineItem {

    @JsonProperty("line_item_id")
    public long lineItemId;

    @JsonProperty("sale_id")
    public long sale;

    @JsonProperty("product_id")
    public long product;

    @JsonProperty("price")
    public String price;

    @JsonProperty("quantity")
    public int quantity;

}
