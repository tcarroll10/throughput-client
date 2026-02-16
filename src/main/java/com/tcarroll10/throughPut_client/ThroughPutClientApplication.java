package com.tcarroll10.throughPut_client;

import com.tcarroll10.throughPut_client.service.RecordSenderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class ThroughPutClientApplication implements CommandLineRunner {

	private final RecordSenderService senderService;

	public ThroughPutClientApplication(RecordSenderService senderService) {
		this.senderService = senderService;
	}

	public static void main(String[] args) {
		SpringApplication.run(ThroughPutClientApplication.class, args);
	}

	@Override
	public void run(String... args) {
		log.info("Throughput client started - beginning send operation");
		senderService.sendAll();
		log.info("Send operation completed - shutting down");
		System.exit(0);
	}
}
