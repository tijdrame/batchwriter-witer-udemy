package com.emard.batchwriter.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.emard.batchwriter.model.Product;

@Component
public class ProductProcessor implements ItemProcessor<Product, Product> {

    @Override
    public Product process(Product product) throws Exception {
        Thread.sleep(300);
        product.setProductDesc(product.getProductDesc().toUpperCase());
        return product;
    }

}
