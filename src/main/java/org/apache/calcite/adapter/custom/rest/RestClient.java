package org.apache.calcite.adapter.custom.rest;


import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;


import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

public class RestClient {

	public String getJsonData(String url){
		String result="";
		Client client = getRestClient();
		WebResource service = client.resource(UriBuilder.fromUri(url).build());
		result = service.accept(MediaType.APPLICATION_JSON)
			.get(String.class);
		System.out.println(result);
		return result;
	}

	public String getJsonData(Client client, String url){
		String result="";
		WebResource service = client.resource(UriBuilder.fromUri(url).build());
		result = service.accept(MediaType.APPLICATION_JSON)
			.get(String.class);
		System.out.println(result);
		return result;
	}

	public Client getRestClient() {
		ClientConfig config = new DefaultClientConfig();
		Client client = Client.create(config);
		return client;
	}

}
