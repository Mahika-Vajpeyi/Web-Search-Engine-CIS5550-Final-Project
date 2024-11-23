package cis5550.webserver;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ResponseImpl implements Response{
	
	byte[] body = null;
	HashMap<String, ArrayList<String>> headers = new HashMap<String, ArrayList<String>>();
	int statusCode = 200;
	String reasonPhrase = "OK";
	OutputStream outputstream;
	boolean committed = false;
	
//	Map from String to ArrayList String
	
//	Add constructor
	
	ResponseImpl(OutputStream outputstream) {
		this.outputstream = outputstream;
//		this.headers = headers;
//		this.statusCode = statusCode;
//		this.reasonPhrase = reasonPhrase;
	}

	@Override
	public void body(String body) {
		this.body = body.getBytes();
	}

	@Override
	public void bodyAsBytes(byte[] bodyArg) {
		this.body = bodyArg;
	}

	@Override
	public void header(String name, String value) {
		if (headers.containsKey(name.toLowerCase())) {
			ArrayList<String> headerValues = headers.get(name.toLowerCase());
			headerValues.add(value);
		} else {
			headers.put(name.toLowerCase(), new ArrayList<String>());
			headers.get(name.toLowerCase()).add(value);
		}
	}

	@Override
	public void type(String contentType) {
		header("content-type", contentType);
	}

	@Override
	public void status(int statusCode, String reasonPhrase) {
		this.statusCode = statusCode;
		this.reasonPhrase = reasonPhrase;
	}

	@Override
	public void write(byte[] b) throws Exception {
		// TODO Auto-generated method stub
		if (!committed) {
			StringBuilder response = new StringBuilder();
			response.append("HTTP/1.1 " + String.valueOf(statusCode) + " " + reasonPhrase+ "\r\n");
//			response.append("Content-Type: " + type + "\r\n");
//			response.append("Server: localhost\r\n");
			for (Map.Entry<String,ArrayList<String>> entry : headers.entrySet()) {
				ArrayList<String> value = entry.getValue();
				for (String s: value) {
					response.append(entry.getKey() + ": " + s + "\r\n");
				}
			}
			response.append("connection: close\r\n\r\n");

//			response.append("Content-Length: " + bodyLength + "\r\n\r\n");
//			System.out.println(response.toString());
			outputstream.write((response.toString()).getBytes());
			committed = true;
		}
		outputstream.write(b, 0, b.length);
		outputstream.flush();
	}

	@Override
	public void redirect(String url, int responseCode) {
		// TODO Auto-generated method stub
		String responseText = "";
		if (responseCode == 301) {
			responseText = "Moved Permanently";
		} else if (responseCode == 302) {
			responseText = "Found";
		} else if (responseCode == 303) {
			responseText = "See Other";
		} else if (responseCode == 307) {
			responseText = "Temporary Redirect";
		} else {
			responseText = "Permanent Redirect";
		}
		
		String response = "HTTP/1.1 " + String.valueOf(responseCode) + " " + responseText + "\r\n"
				+ "Location: " + url + "\r\n\r\n";
		try {
			outputstream.write((response.toString()).getBytes());
			outputstream.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void halt(int statusCode, String reasonPhrase) {
		// TODO Auto-generated method stub
		
	}
	
	public int bodyLength() {
		if(body != null) {
			return body.length;
		}
		return 0;
	}
	
	public String getType() {
		if (headers.get("content-type") != null) {
			return headers.get("content-type").get(0);
		}
		return "application/octet-stream";
	}
	
	public byte[] getBody() {
		return body;
	}
	
	public HashMap<String, ArrayList<String>> getHeaders (){
		return headers;
	}
	
	public int getStatusCode() {
		return statusCode;
	}
	
	public String getReasonPhrase() {
		return reasonPhrase;
	}
	
	public boolean isCommitted() {
		return committed;
	}
}
