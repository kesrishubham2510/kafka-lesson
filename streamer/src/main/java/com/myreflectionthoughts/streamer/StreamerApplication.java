package com.myreflectionthoughts.streamer;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.myreflectionthoughts.streamer.*")
public class StreamerApplication {

	public static void main(String[] args) {

		SpringApplication.run(StreamerApplication.class, args);
	}
}
