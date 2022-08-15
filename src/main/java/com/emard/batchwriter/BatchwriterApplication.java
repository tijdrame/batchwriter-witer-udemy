package com.emard.batchwriter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BatchwriterApplication {

	public static void main(String[] args) {
		System.exit(SpringApplication.exit(
			SpringApplication.run(BatchwriterApplication.class, args)));
	}

}
