package com.emard.batchwriter.model;

import java.math.BigDecimal;

import com.thoughtworks.xstream.annotations.XStreamAlias;

public class Product {
    @XStreamAlias("p_id")
    private Integer productID;
    private String productName;
    private String productDesc;
    private BigDecimal price;
    private Integer unit;

    public Product() {
    }

    public Product(Integer productID, String productName, String productDesc, BigDecimal price, Integer unit) {
        this.productID = productID;
        this.productName = productName;
        this.productDesc = productDesc;
        this.price = price;
        this.unit = unit;
    }

    public Integer getProductID() {
        return this.productID;
    }

    public void setProductID(Integer productID) {
        this.productID = productID;
    }

    public String getProductName() {
        return this.productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductDesc() {
        return this.productDesc;
    }

    public void setProductDesc(String productDesc) {
        this.productDesc = productDesc;
    }

    public BigDecimal getPrice() {
        return this.price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public Integer getUnit() {
        return this.unit;
    }

    public void setUnit(Integer unit) {
        this.unit = unit;
    }

    public Product productID(Integer productID) {
        setProductID(productID);
        return this;
    }

    public Product productName(String productName) {
        setProductName(productName);
        return this;
    }

    public Product productDesc(String productDesc) {
        setProductDesc(productDesc);
        return this;
    }

    public Product price(BigDecimal price) {
        setPrice(price);
        return this;
    }

    public Product unit(Integer unit) {
        setUnit(unit);
        return this;
    }

    @Override
    public String toString() {
        return "{" +
            " productID='" + getProductID() + "'" +
            ", productName='" + getProductName() + "'" +
            ", productDesc='" + getProductDesc() + "'" +
            ", price='" + getPrice() + "'" +
            ", unit='" + getUnit() + "'" +
            "}";
    }

}

