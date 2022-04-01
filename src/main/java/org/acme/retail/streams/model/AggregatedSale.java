package org.acme.retail.streams.model;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AggregatedSale {

    @JsonProperty("sale_id")
    public long saleId;

    @JsonProperty("customer_id")
    public Long customer;

    @JsonProperty("date")
    public Long date;

    @JsonProperty("total")
    public String totalPrice;

    public AggregatedSale addLineItem(SaleAndLineItem saleAndLineItem) {
        this.saleId = saleAndLineItem.sale.saleId;
        this.customer = saleAndLineItem.sale.customer;
        this.date = saleAndLineItem.sale.date;
        if (this.totalPrice == null || this.totalPrice.isEmpty()) {
            this.totalPrice = new BigDecimal("0").toPlainString();
        }
        this.totalPrice = new BigDecimal(this.totalPrice)
                .add(new BigDecimal(saleAndLineItem.lineItem.price).multiply(new BigDecimal(saleAndLineItem.lineItem.quantity))).toPlainString();
        return this;
    }

    public AggregatedSale removeLineItem(SaleAndLineItem saleAndLineItem) {
        if (this.totalPrice == null || this.totalPrice.isEmpty()) {
            this.totalPrice = new BigDecimal(0).toPlainString();
        }
        this.totalPrice = new BigDecimal(this.totalPrice)
                .subtract(new BigDecimal(saleAndLineItem.lineItem.price).multiply(new BigDecimal(saleAndLineItem.lineItem.quantity))).toPlainString();
        return this;
    }

}
