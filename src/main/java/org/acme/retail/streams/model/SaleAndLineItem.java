package org.acme.retail.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SaleAndLineItem {

    @JsonProperty("sale")
    public Sale sale;

    @JsonProperty("line_item")
    public LineItem lineItem;

    public SaleAndLineItem() {}

    public SaleAndLineItem(Sale sale, LineItem lineItem) {
        this.sale = sale;
        this.lineItem = lineItem;
    }
}
