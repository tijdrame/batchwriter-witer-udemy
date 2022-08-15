package com.emard.batchwriter.service;

import org.springframework.web.client.RestTemplate;

import com.emard.batchwriter.model.Product;

//@Service
public class ProductService {
    public Product getProduct() {
        String url = "http://localhost:8081/api/product";
        RestTemplate restTemplate = new RestTemplate();
        Product p = restTemplate.getForObject(url, Product.class);
        return p;
    }
}
