package com.emard.batchwriter.reader;

import org.springframework.stereotype.Component;

import com.emard.batchwriter.model.Product;
import com.emard.batchwriter.service.ProductService;

import lombok.extern.slf4j.Slf4j;

//@Component
@Slf4j
public class ProductServiceAdapter {
    private final ProductService service;

    public ProductServiceAdapter(ProductService service) {
        this.service = service;
    }

    public Product nextProduct() throws InterruptedException {
        Product p = null;
        Thread.sleep(1000);
        try {
            p = this.service.getProduct();
            log.info("connected web service ... ok");
        } catch (Exception e) {
            log.info("exception [{}]", e.getMessage());
            throw e;
        }
        return p;
    }
}
