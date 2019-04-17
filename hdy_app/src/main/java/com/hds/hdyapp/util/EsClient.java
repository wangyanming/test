package com.hds.hdyapp.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class EsClient {
	//private static final String HOST = "192.168.1.201";
	//private static final String HOST = "192.168.0.29";
	private static final String HOST = "39.107.79.26";
	//private static final String CLUSTERNAME = "evente-elasticsearch";
	private static final String CLUSTERNAME = "bi-es";
	private static final int PORT = 9300;
	private static volatile TransportClient client;
	private static final Settings settings = Settings.builder()
			.put("cluster.name", CLUSTERNAME)
			//.put("client.transport.sniff", true)
			.build();
	
	private EsClient() {
		
	}
	
	@SuppressWarnings("resource")
	public static TransportClient getConnect() {
		if (client == null) {
			synchronized (EsClient.class) {
				if (client == null) {
					try {
						client = new PreBuiltTransportClient(settings)
								.addTransportAddress(new TransportAddress(InetAddress.getByName(HOST),PORT));
								//.addTransportAddress(new TransportAddress(InetAddress.getByName(HOST),PORT))
								//.addTransportAddress(new TransportAddress(InetAddress.getByName(HOST),PORT));
					} catch (UnknownHostException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return client;
	}
}
