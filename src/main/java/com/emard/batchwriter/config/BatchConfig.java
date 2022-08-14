package com.emard.batchwriter.config;

import java.io.IOException;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.web.client.ResourceAccessException;

import com.emard.batchwriter.listener.ProductSkipListener;
import com.emard.batchwriter.model.Product;
import com.emard.batchwriter.processor.ProductProcessor;
import com.emard.batchwriter.reader.ProductServiceAdapter;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private final StepBuilderFactory stepBuilder;
    private final JobBuilderFactory jobBuilder;
    private final DataSource dataSource;
    private final ProductSkipListener productSkipListener;
    private final ProductProcessor processor;
    //private final ProductServiceAdapter productAdapter;

    public BatchConfig(StepBuilderFactory stepBuilder, JobBuilderFactory jobBuilder,
    DataSource dataSource, ProductSkipListener productSkipListener,
    ProductProcessor processor/*, ProductServiceAdapter productAdapter */) {
        this.stepBuilder = stepBuilder;
        this.jobBuilder = jobBuilder;
        this.dataSource = dataSource;
        this.productSkipListener = productSkipListener;
        this.processor = processor;
        //this.productAdapter = productAdapter;
    }

    /*@Bean
    public ItemReaderAdapter<Product> serviceAdapter(){
        ItemReaderAdapter<Product> reader = new ItemReaderAdapter<>();
        reader.setTargetObject(productAdapter);
        reader.setTargetMethod("nextProduct");
        return reader;
    }*/

    @StepScope
    @Bean
    public FlatFileItemReader<Product> flatFileSimpleItemReader(
            @Value("#{jobParameters['fileInput']}") FileSystemResource inputFile) {
        return new FlatFileItemReaderBuilder<Product>()
                .name("flatFileFixedItemReader")
                .linesToSkip(1)
                .resource(inputFile)
                .delimited()
                .names("productID", "productName", "productDesc", "price", "unit")
                .targetType(Product.class)
                .build();
    }

    @StepScope
    @Bean
    public FlatFileItemWriter<Product> flatFileItemWriter(
        @Value("#{jobParameters['fileOutput']}") FileSystemResource outputFile
    ){
        FlatFileItemWriter<Product> writer = new FlatFileItemWriter<>(){
            public String doWrite(List<? extends Product> list){
                for (Product it : list) {
                    if(it.getProductID()==9) throw new RuntimeException("Because Id = 9");
                }
                return super.doWrite(list);
            }
        };
        writer =new FlatFileItemWriterBuilder<Product>()
        .name("flatFileItemWriter")
        .resource(outputFile)
        
        .delimited().delimiter("|")
        .names("productID", "productName", "productDesc", "price", "unit")
        
        //.append(true)
        .footerCallback(new FlatFileFooterCallback() {

            @Override
            public void writeFooter(Writer writer) throws IOException {
                writer.write("This file was created at "+ LocalDate.now());
            }
            
        })
        .headerCallback(new FlatFileHeaderCallback(){
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write("PROD_ID|"+"PROD_NAME|"+"PROD_DESC|"+"PRICE|"+"UNIT|");
            }
        }).build();

        return writer;
    }

    @StepScope
    @Bean
    public StaxEventItemWriter<Product> xmlWriter(
        @Value("#{jobParameters['fileOutputXml']}") FileSystemResource outputFile
    ){
        XStreamMarshaller marshaller =  new XStreamMarshaller();
        Map<String, Class<Product>> aliases = new HashMap<>();
        aliases.put("product", Product.class);
        marshaller.setAliases(aliases); // remplacer le com....model.Prodruct par Product
        marshaller.setAutodetectAnnotations(true);//prendre les anno de la classe
        return new StaxEventItemWriterBuilder<Product>()
            .name("xmlWriter")
            .resource(outputFile)
            .marshaller(marshaller)
            .rootTagName("Products")
            .build();
    }

    @Bean
    public JdbcBatchItemWriter<Product> jdbcWriter() {
        return new JdbcBatchItemWriterBuilder<Product>()
            .dataSource(dataSource)
            .sql("insert into products (product_id, product_name, product_desc, price, unit)"+
            " values (?, ?, ?, ?, ?)")
            .itemPreparedStatementSetter(new ItemPreparedStatementSetter<Product>() {
                @Override
                public void setValues(Product p, PreparedStatement ps) throws SQLException {
                    ps.setInt(1, p.getProductID());
                    ps.setString(2, p.getProductName());
                    ps.setString(3, p.getProductDesc());
                    ps.setBigDecimal(4, p.getPrice());
                    ps.setInt(5, p.getUnit());
                }
            })
            .build();
    }

    @Bean
    public JdbcBatchItemWriter<Product> jdbcWriter2() {
        return new JdbcBatchItemWriterBuilder<Product>()
            .dataSource(dataSource)
            .sql("insert into products (product_id, product_name, product_desc, price, unit)"+
            " values (:productID, :productName, :productDesc, :price, :unit)") //col pojo
            .beanMapped()
            .build();
    }

    @Bean
    public Step step1() {
        return stepBuilder.get("step1")
                .<Product, Product>chunk(3)
                .reader(flatFileSimpleItemReader(null))
                .processor(processor)
                //.writer(flatFileItemWriter(null))
                .writer(jdbcWriter2())
                .faultTolerant()
                //.retry(FlatFileParseException.class)
                //.retryLimit(5)
                .skip(FlatFileParseException.class)
                .skipLimit(3)
                //.skipPolicy(new AlwaysSkipItemSkipPolicy())
                //.skip(FlatFileParseException.class)
                //.skip(RuntimeException.class)
                //.skipLimit(20)//nb d'error autorisé
                //.skipPolicy(new AlwaysSkipItemSkipPolicy())
                //.listener(productSkipListener)
                .build();
    }

    @Bean
    public Step step0(){
        return stepBuilder.get("step0")
        .tasklet(new ConsoleTasklet())
        .build();
    }

    @Bean
    public Job helloWorldJob() {
        return jobBuilder.get("helloWorldJob")
                .incrementer(new RunIdIncrementer())
                //.listener(executionListener)
                .start(step0())
                .next(step1())
                .build();
    }
}
