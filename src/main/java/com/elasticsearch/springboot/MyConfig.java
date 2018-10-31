package com.elasticsearch.springboot;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MyConfig {

	@Bean
	public TransportClient client() throws UnknownHostException{
		
		InetSocketTransportAddress node = new InetSocketTransportAddress(InetAddress.getByName("192.168.2.200"),9300);
		
		Settings settins = Settings.builder().put("cluster.name", "wangxin").build();
		
		TransportClient client = new PreBuiltTransportClient(settins);
		client.addTransportAddress(node);
		return client;
	}
}
