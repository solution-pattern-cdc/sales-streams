package org.acme.retail.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Sale {

    @JsonProperty("sale_id")
    public long saleId;

    @JsonProperty("customer_id")
    public Long customer;

    @JsonProperty("date")
    public Long date;

}
