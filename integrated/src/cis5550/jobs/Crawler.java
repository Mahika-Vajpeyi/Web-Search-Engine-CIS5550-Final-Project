package cis5550.jobs;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class Crawler {
	public static List<String> urlExtract(byte[] data) {
		ArrayList<String> urls = new ArrayList<String>();
		String htmlText = new String(data);
		Pattern pattern = Pattern.compile("<([^/][^>]*)>");
		Matcher matcher = pattern.matcher(htmlText);
		while (matcher.find()) {
			String tag = matcher.group(1);
			String[] tagParts = tag.split("\\s+");
			String tagType = tagParts[0];
			if (tagType.toLowerCase().equals("a")) {
				for (String s: tagParts) {
					if(s.contains("href=")) {
						int idx = s.indexOf('=');
						String url = s.substring(idx + 1);
						if (url.length() > 1) {
							String finalUrl = url.substring(1, url.length() - 1);
							urls.add(finalUrl);
						}
					}
				}
			}
		}
		return urls;
	}
	
	public static List<String> urlNormalize(List<String> urls, String base) {
		ArrayList<String> normalizedUrls = new ArrayList<String>();
		String baseUrl = base;
		for (String url : urls) {
			String[] urlParts = URLParser.parseURL(url);
			String protocol = urlParts[0];
			String hostName = urlParts[1];
			String port = urlParts[2];
			String rest = urlParts[3];
			if (rest.indexOf('#') > -1) {
				rest = rest.substring(0, rest.indexOf('#'));
				if (protocol == null && hostName == null && port == null && rest.equals("")) {
					normalizedUrls.add(baseUrl);
					continue;
				}
			}
			if (rest.lastIndexOf('.') > -1) {
				String end = rest.substring(rest.lastIndexOf('.'));
				if (end.equals(".jpg") || end.equals(".jpeg") || end.equals(".gif") || end.equals(".png") || end.equals(".txt")) {
					continue;
				}
			}
			if (protocol == null && hostName == null && port == null) {
//				Either relative or absolute path without hostname
//				Cuts off part in base URL after last /
				String modifiedBaseUrl = baseUrl.substring(0, baseUrl.lastIndexOf('/'));
				if (rest.length() == 0 || rest.charAt(0) == '/') {
//					absolute path
					String[] baseUrlParts = URLParser.parseURL(baseUrl);
					modifiedBaseUrl = baseUrlParts[0] + "://" + baseUrlParts[1] + ":" + baseUrlParts[2];
					normalizedUrls.add(modifiedBaseUrl+rest);
					continue;
				} else {
					while (rest.length() >= 3 && rest.substring(0, 3).equals("../")) {
						modifiedBaseUrl = modifiedBaseUrl.substring(0, modifiedBaseUrl.lastIndexOf('/'));
						rest = rest.substring(3);
					}
					normalizedUrls.add(modifiedBaseUrl+"/"+rest);
					continue;
				}
			}
			else if (port == null) {
				String modifiedBaseUrl = "";
				if (protocol.equals("http")) {
					modifiedBaseUrl = protocol + "://" + hostName + ":" + "80";
				} else if (protocol.equals("https")){
					modifiedBaseUrl = protocol + "://" + hostName + ":" + "443";
				} else {
					continue;
				}
				normalizedUrls.add(modifiedBaseUrl+rest);
				continue;
			}
			normalizedUrls.add(url);
		}
		return normalizedUrls;
	}
	
	public static String urlSeedNormalize(String base) {
		String url = base;
		String[] urlParts = URLParser.parseURL(base);
		String protocol = urlParts[0];
		String hostName = urlParts[1];
		String port = urlParts[2];
		String rest = urlParts[3];
		String modifiedBaseUrl = "";
		if (rest.indexOf('#') > -1) {
			rest = rest.substring(0, url.indexOf('#'));
		}
		if (port == null) {
			if (protocol.equals("http")) {
				modifiedBaseUrl = protocol + "://" + hostName + ":" + "80" + rest;
			} else if (protocol.equals("https")){
				modifiedBaseUrl = protocol + "://" + hostName + ":" + "443" + rest;
			} 
			return modifiedBaseUrl;
		}
		return protocol + "://" + hostName + ":" + port + rest;
	}
	
//	public static boolean cleanRobot(String txt) {
//		i
//		return false;
//	}
	
	public static boolean robotProcessUrl(String txt, String rest) {
		String[] lines = txt.split("\n");
		
		for (int i=0; i<lines.length; i++) {
			String currLine = lines[i];
			if (currLine.equals("User-agent: cis5550-crawler")) {
//				If find corresponding User-agent, loop through rest of lines until end of next User-agent
				for (int j=i+1; j<lines.length && !lines[j].equals(""); j++) {
					String condLine = lines[j];
					int colonIdx = condLine.indexOf(':');
					if (colonIdx != -1 && colonIdx + 2 < condLine.length() && rest.startsWith(condLine.substring(colonIdx+2))){
//						is a prefix, so check if allowed or disallowed
						if (condLine.substring(0, colonIdx).equals("Disallow")) return false;
						else return true;
					}
				}
			}
		}		
		for (int i=0; i<lines.length; i++) {
			String currLine = lines[i];
			if (currLine.equals("User-agent: *")) {
//				If find corresponding User-agent, loop through rest of lines until end of next User-agent
				for (int j=i+1; j<lines.length && !lines[j].equals(""); j++) {
					String condLine = lines[j];
					int colonIdx = condLine.indexOf(':');
					if (colonIdx != -1 && colonIdx + 2 < condLine.length() && rest.startsWith(condLine.substring(colonIdx+2))){
//						is a prefix, so check if allowed or disallowed
						if (condLine.substring(0, colonIdx).equals("Disallow")) return false;
						else return true;
					}
				}
			}
		}
		return true;
	}
	
	public static float robotProcessDelay(String txt) {
		String[] lines = txt.split("\n");
		
		for (int i=0; i<lines.length; i++) {
			String currLine = lines[i];
			if (currLine.equals("User-agent: cis5550-crawler")) {
//				If find corresponding User-agent, loop through rest of lines until end of next User-agent
				for (int j=i+1; j<lines.length && !lines[j].equals(""); j++) {
					String condLine = lines[j];
					int colonIdx = condLine.indexOf(':');
					if (condLine.substring(0, colonIdx).equals("Crawl-delay")){
//						is a prefix, so check if allowed or disallowed
						String val = condLine.substring(colonIdx + 1);
						return Float.parseFloat(val)*1000;
					}
				}
			}
		}		
		for (int i=0; i<lines.length; i++) {
			String currLine = lines[i];
			if (currLine.equals("User-agent: *")) {
//				If find corresponding User-agent, loop through rest of lines until end of next User-agent
				for (int j=i+1; j<lines.length && !lines[j].equals(""); j++) {
					String condLine = lines[j];
					int colonIdx = condLine.indexOf(':');
					if (colonIdx != -1 && condLine.substring(0, colonIdx).equals("Crawl-delay")){
//						is a prefix, so check if allowed or disallowed
						String val = condLine.substring(colonIdx + 1);
						return Float.parseFloat(val)*1000;
					}
				}
			}
		}	
		return 1000;
	}
	
	public static void run(FlameContext ctx, String[] strArr) {
		if (strArr.length < 1) {
			ctx.output("Insufficient arguments");
		} else {
			ctx.output("OK");
		}
		String seedUrl = strArr[0];
		String normalizedSeedUrl = urlSeedNormalize(seedUrl);
		try {
			FlameRDD urlQueue = ctx.parallelize(Arrays.asList(normalizedSeedUrl));
			
			StringToIterable lambda = s -> {
				KVSClient kvs = ctx.getKVS();
				List<String> urlStrings = new ArrayList<String>();
				List<String> normalizedUrlStrings = new ArrayList<String>();
				
//				Already visited
				if (kvs.existsRow("pt-crawl", Hasher.hash(s))) {
					return normalizedUrlStrings;
				}
				
				
				URL url = new URL(s);
				
				String hashedUrl = Hasher.hash(s);
				Row r = new Row(hashedUrl);
				
				String[] urlParts = URLParser.parseURL(s);
				String hostName = urlParts[1];
				String sRest = urlParts[3];
				
				if(kvs.get("hosts", hostName, "robot") != null){
//					Already has robots.txt entry
//					HANDLE FILTERING
					String txt = new String(kvs.get("hosts", hostName, "robot"));
					boolean allowable = robotProcessUrl(txt, sRest);
					if (!allowable) {
						return normalizedUrlStrings;
					}
				} else {
//					Download robots.txt
					String robotUrlString  = urlParts[0]+"://"+hostName+":"+urlParts[2]+"/robots.txt";
					URL robotUrl = new URL(robotUrlString);
					HttpURLConnection connectRobot = (HttpURLConnection) robotUrl.openConnection();
					connectRobot.setInstanceFollowRedirects(false);
					connectRobot.setRequestMethod("GET");
					connectRobot.setRequestProperty("User-Agent", "cis5550-crawler");
					try {
						connectRobot.connect();
					} catch (Exception e) {
						return normalizedUrlStrings;
					}
					
					if(connectRobot.getResponseCode() == 200) {
						ByteArrayOutputStream op = new ByteArrayOutputStream();
						BufferedInputStream ip = new BufferedInputStream(connectRobot.getInputStream());
						int data;
						while ((data = ip.read()) != -1) {
							op.write(data);
						}
						
						byte[] buffer = op.toByteArray();
						String txt = new String(buffer);
						kvs.put("hosts", hostName, "robot", txt);
						float delay = robotProcessDelay(txt);
						kvs.put("hosts", hostName, "delay", Float.toString(delay));
					} else {
						kvs.put("hosts", hostName, "robot", "NONE");
						kvs.put("hosts", hostName, "delay", "1000");
					}
//					HANDLE FILTERING (IF NEED)
					String txt = new String(kvs.get("hosts", hostName, "robot"));
					boolean allowable = robotProcessUrl(txt, sRest);
					if (!allowable) {
						return normalizedUrlStrings;
					}
				}
				
				
//				Check last visited
				if (kvs.get("hosts", hostName, "value") != null) {
					String delay = new String(kvs.get("hosts", hostName, "delay"));
					float delayFloat = Float.parseFloat(delay);
//					System.out.println(new String(kvs.get("hosts", hostName, "value"))+ " delay " + String.valueOf(delayFloat) + " current " +String.valueOf(System.currentTimeMillis()));
					if (delayFloat >= System.currentTimeMillis() - Long.parseLong(new String(kvs.get("hosts", hostName, "value")))) {
//						System.out.println("asdf");
//						System.out.println(new String(Long.parseLong(new String(kvs.get("hosts", hostName, "value")))+delayFloat) + " current " +String.valueOf(System.currentTimeMillis()));
						normalizedUrlStrings.add(s);
						return normalizedUrlStrings;
					}
				} 
				
//				Either doesn't exist (so first time), or exists but can make another request
				kvs.put("hosts", hostName, "value", Long.toString(System.currentTimeMillis()));
				
//				HEAD REQUEST
				try {
					HttpURLConnection connectHead = (HttpURLConnection) url.openConnection();
					connectHead.setInstanceFollowRedirects(false);
					connectHead.setRequestMethod("HEAD");
					connectHead.setRequestProperty("User-Agent", "cis5550-crawler");
					connectHead.connect();
										
					r.put("url", s);
					r.put("responseCode", Integer.toString(connectHead.getResponseCode()));
					if(connectHead.getContentType() != null) {
						r.put("contentType", connectHead.getContentType());
					}
					if(connectHead.getContentLength() != -1) {
						r.put("length", Integer.toString(connectHead.getContentLength()));
					}
					kvs.putRow("pt-crawl", r);
					if(connectHead.getResponseCode() != 200 && connectHead.getResponseCode() != 301 
							&& connectHead.getResponseCode() != 302 && connectHead.getResponseCode() != 303
							&& connectHead.getResponseCode() != 307 && connectHead.getResponseCode() != 308) {
//						if none of the expected codes returning empty set
						return normalizedUrlStrings;
					}
					
					if(connectHead.getResponseCode() == 301 
							|| connectHead.getResponseCode() == 302 || connectHead.getResponseCode() == 303
							|| connectHead.getResponseCode() == 307 || connectHead.getResponseCode() == 308) {
						String newUrl = connectHead.getHeaderField("Location");
						ArrayList<String> normList = new ArrayList<String>();
						normList.add(newUrl);
						List<String> normalizedUrl = urlNormalize(normList, s);
						normalizedUrlStrings.add(normalizedUrl.get(0));
						return normalizedUrlStrings;
					}

					if(connectHead.getResponseCode() == 200 && connectHead.getContentType().contains("text/html")) {
						HttpURLConnection connect = (HttpURLConnection) url.openConnection();
						connect.setInstanceFollowRedirects(false);
						connect.setRequestMethod("GET");
						connect.setRequestProperty("User-Agent", "cis5550-crawler");
						connect.connect();
						if(connect.getResponseCode() == 200) {
							ByteArrayOutputStream op = new ByteArrayOutputStream();
							BufferedInputStream ip = new BufferedInputStream(connect.getInputStream());
							int data;
							while ((data = ip.read()) != -1) {
								op.write(data);
							}
							
							byte[] buffer = op.toByteArray();
							
							
							r.put("page", buffer);
							r.put("responseCode", Integer.toString(connect.getResponseCode()));
							kvs.putRow("pt-crawl", r);
							urlStrings = urlExtract(buffer);
							normalizedUrlStrings = urlNormalize(urlStrings, s);
						}
						return normalizedUrlStrings;
					}
				} catch (Exception e) {
					e.printStackTrace();
					return normalizedUrlStrings;
				}
				return normalizedUrlStrings;
			};
			
			while (urlQueue.count() > 0) {
				urlQueue = urlQueue.flatMap(lambda);
//				Thread.sleep(50);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
