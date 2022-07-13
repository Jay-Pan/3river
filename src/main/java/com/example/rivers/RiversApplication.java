package com.example.rivers;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@Slf4j
public class RiversApplication {


	public static void main(String[] args) {
		SpringApplication.run(RiversApplication.class, args);
		log.info("Rivers application started.");
	}


	@Bean
	public KStream<String, GenericRecord> kStream (StreamsBuilder kStreamBuilder) {
		KStream<String, GenericRecord> stream = kStreamBuilder.stream("Customer");
		stream
				.map(new hoistAccountIdMapper())
//				.map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
//					@Override
//					public KeyValue<String, String> apply(String key, String value) {
//						return new KeyValue<>("KEY-Static", value);
//					}
//				})
//				.mapValues((ValueMapper<String, String>) String::toUpperCase)
				.to("CustomerReKeyed");
		stream.print(Printed.toSysOut());
		return stream;
	}

	public static class hoistAccountIdMapper implements KeyValueMapper<String, GenericRecord, KeyValue<String, GenericRecord>> {

		@Override
		public KeyValue<String, GenericRecord> apply(String key, GenericRecord value) {
			log.info(value.toString());
			String accountId = (String) value.get("accountId");
			return new KeyValue<>(accountId, value);
		}
	}

}
