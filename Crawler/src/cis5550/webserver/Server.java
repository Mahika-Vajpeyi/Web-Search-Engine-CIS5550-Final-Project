package cis5550.webserver;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.HashMap;
import java.util.Iterator;

import javax.net.ssl.*;
import java.security.*;
import javax.net.ServerSocketFactory;

import cis5550.tools.Logger;

public class Server extends Thread {
		
//	private static final Logger logger = Logger.getLogger(Server.class);
	
	protected static final int NUM_WORKERS = 100;
	
	protected static int contentLength(ArrayList<String> headerLines) {
		for (String s : headerLines) {
			String sLower = s.toLowerCase();
			if(sLower.contains("content-length")) {
				String length = s.substring(16);
				return Integer.parseInt(length);
			}
		}
		return 0;
	}
	
	protected static boolean closeConnection(ArrayList<String> headerLines) {
		for (String s : headerLines) {
			String sLower = s.toLowerCase();
			if(sLower.contains("connection")) {
				String status = s.substring(12);
				return status.equals("close");
			}
		}
		return false;
	}
	
	protected static String filePath(String headerLine) {
		String[] words = headerLine.split(" ");
		return words[1];
	}
	
	protected static String requestMethod(ArrayList<String> headerLines) {
		String firstLine = headerLines.get(0);
		if (firstLine.contains("GET")) {
			return "GET";
		}
		if (firstLine.contains("PUT")) {
			return "PUT";
		}
		if (firstLine.contains("POST")) {
			return "POST";
		}
//		Shouldn't have other ones because headerCheck will give 501
		return "HEAD";
	}
	
	protected static String requestProtocol(ArrayList<String> headerLines) {
		String firstLine = headerLines.get(0);
		String[] firstLineArray = firstLine.split(" ");
		return firstLineArray[2];
	}
	
	protected static int headerCheck(ArrayList<String> headerLines) {
		String[] firstLineArray = headerLines.get(0).split(" ");
		String firstLine = headerLines.get(0);
//		missing method, URL, protocol, or Host
		if (firstLineArray.length != 3 || headerLines.size() < 2) {
//			System.out.println(firstLine.length);
			return 400;
		}
		if (firstLineArray[1].contains("..")) {
			return 403;
		}
		if (!firstLine.contains("HTTP/")) {
			return 400;
		}
//		if (firstLine.contains("POST") || firstLine.contains("PUT")) {
//			return 405;
//		}
		if (!(firstLine.contains("GET") || firstLine.contains("HEAD") || firstLine.contains("POST") || firstLine.contains("PUT"))) {
			return 501;
		}
		if (!firstLine.contains("HTTP/1.1")) {
			return 505;
		}
		
		boolean hostExist = false;
		for (String s : headerLines) {
			if (s.toLowerCase().contains("host:")) {
				hostExist = true;
			}
		}
		if (!hostExist) return 400;
		
		return 200;
	}
	
	protected static boolean getOrHead(ArrayList<String> headerLines) {
//		true for get, false for head
		if (headerLines.get(0).contains("GET")) {
			return true;
		} else return false;
	}
	
	protected static String contentType(ArrayList<String> headerLines) {
//		true for get, false for head
		String[] firstLineArray = headerLines.get(0).split(" ");
		if (firstLineArray[1].contains(".jpg") || firstLineArray[1].contains(".jpeg")) {
			return "image/jpeg";
		} else if (firstLineArray[1].contains(".txt")) {
			return "text/plain";
		} else if (firstLineArray[1].contains(".html")) {
			return "text/html";
		} else {
			return "application/octet-stream";
		}
	}
	
	protected static String modifiedSince(ArrayList<String> headerLines) {
//		true for get, false for head
		for (String s : headerLines) {
			String sLower = s.toLowerCase();
			if(sLower.contains("if-modified-since")) {
				String date = s.substring(19);
				return date;
			}
		}
		return "NONE";
	}
	
	protected static Map<String,String> headersParse(ArrayList<String> headerLines) {
//		true for get, false for head
		Map<String, String> headers = new HashMap<String, String>();
		for (int i = 1; i<headerLines.size(); i++) {
			String s = headerLines.get(i);
			String[] keyValue = s.split(":\\s");
			String key = keyValue[0].toLowerCase();
			headers.put(key, keyValue[1]);
		}
		return headers;
	}
	
	protected static Map<String,String> pathMatch(String filepath, String pathPattern) {
		String path = filepath;
//		If have query params ignore for path matching
		if (filepath.indexOf("?") != -1) {
			path = path.substring(0, filepath.indexOf("?"));
		}
		String[] patternSplit = pathPattern.split("/");
		String[] pathSplit = path.split("/");
		if(pathSplit.length != patternSplit.length) {
			return null;
		}
		Map<String,String> params = new HashMap<String, String>();
		boolean match = true;
		for (int i = 1; i<patternSplit.length; i++) {
			if (patternSplit[i].charAt(0) != ':' && !patternSplit[i].equals(pathSplit[i])) {
				match = false;
			}
			if (patternSplit[i].charAt(0) == ':') {
				params.put(patternSplit[i].substring(1), pathSplit[i]);
			}
		}
		if (!match) return null;
		return params;
	}
	
	protected static Map<String,String> processQueryParams(String filepath, String body) {
		String queryParams = filepath;
		Map<String,String> params = new HashMap<String, String>();
//		Process query params in URL
		if (filepath.indexOf("?") != -1) {
			queryParams = queryParams.substring(filepath.indexOf("?")+1);
			String[] pairs = queryParams.split("&");
			for (int i = 0; i < pairs.length; i++) {
				String pair = pairs[i];
				String[] keyValue = pair.split("=");
				if (keyValue.length == 2) {
					params.put(java.net.URLDecoder.decode(keyValue[0]), java.net.URLDecoder.decode(keyValue[1]));
				} else {
					params.put(java.net.URLDecoder.decode(keyValue[0]), "");
				}
			}
		}
//		Process query params in body (if any)
		if (body != null) {
			String[] pairs = body.split("&");
			for (int i = 0; i < pairs.length; i++) {
				String pair = pairs[i];
				String[] keyValue = pair.split("=");
				if (keyValue.length == 2) {
					params.put(java.net.URLDecoder.decode(keyValue[0]), java.net.URLDecoder.decode(keyValue[1]));
				} else {
					params.put(java.net.URLDecoder.decode(keyValue[0]), "");
				}
			}
		}
		
		return params;
	}

	
//	HW 2 INTIALIZE STUFF
	
	static Server serverInstance = null;
//	static Server httpsServerInstance = null;
	static boolean threadFlag = false;
	static int port = 80;
	static String directory = ""; 
	static HashMap<String, HashMap<String, Route>> routingTable = new HashMap<String, HashMap<String, Route>>();
	static int securePortNo;
	static HashMap<String, SessionImpl> sessions = new HashMap<String, SessionImpl>();
	
	static boolean secureServerType;
	
	public static class staticFiles {
		public static void location(String s) {
			if (serverInstance == null) {
				Server instance = new Server();
				serverInstance = instance;
			}
			directory = s;
			if (!threadFlag) {
				serverInstance.start();
				threadFlag = true;
//				If not threadFlag, then implies this is first time starting
//				First time starting implies no call to route before
//				If no call to route before then should start https, otherwise don't need to start again
//				if (httpsServerInstance != null) {
//					httpsServerInstance.start();
//				}
			}
		}
	}
	
	public static void get(String s, Route r) {
		if (serverInstance == null) {
			Server instance = new Server();
			serverInstance = instance;
		}
		if (!threadFlag) {
			serverInstance.start();
			threadFlag = true;
//			if (httpsServerInstance != null) {
//				httpsServerInstance.start();
//			}
		}
		
		if (routingTable.containsKey("GET")) {
			HashMap<String, Route> methodRoutes = routingTable.get("GET");
			methodRoutes.putIfAbsent(s, r);
		} else {
			routingTable.put("GET", new HashMap<String, Route>());
			routingTable.get("GET").put(s, r);
		}
	}
	
	public static void post(String s, Route r) {
		if (serverInstance == null) {
			Server instance = new Server();
			serverInstance = instance;
		}
		if (!threadFlag) {
			serverInstance.start();
			threadFlag = true;
//			if (httpsServerInstance != null) {
//				httpsServerInstance.start();
//			}
		}
		
		if (routingTable.containsKey("POST")) {
			HashMap<String, Route> methodRoutes = routingTable.get("POST");
			methodRoutes.putIfAbsent(s, r);
		} else {
			routingTable.put("POST", new HashMap<String, Route>());
			routingTable.get("POST").put(s, r);
		}
	}
	
	public static void put(String s, Route r) {
		if (serverInstance == null) {
			Server instance = new Server();
			serverInstance = instance;
		}
		if (!threadFlag) {
			serverInstance.start();
			threadFlag = true;
//			if (httpsServerInstance != null) {
//				httpsServerInstance.start();
//			}
		}
		
		if (routingTable.containsKey("PUT")) {
			HashMap<String, Route> methodRoutes = routingTable.get("PUT");
			methodRoutes.putIfAbsent(s, r);
		} else {
			routingTable.put("PUT", new HashMap<String, Route>());
			routingTable.get("PUT").put(s, r);
		}
	}
	
	public static void port(int p) {
		if (serverInstance == null) {
			Server instance = new Server();
			serverInstance = instance;
		}
		port = p;
	}
	
	public static void securePort(int p) {
//		if (httpsServerInstance == null) {
//			Server httpsInstance = new Server(true);
//			httpsServerInstance = httpsInstance;
//		}
		secureServerType = true;
		securePortNo = p;
	}
	
	public static void serverLoop(ServerSocket s, BlockingQueue<Socket> socketQueue) {
		while(true) {
			Socket sock = null;
			try {
				sock = s.accept();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
//			ADD TO BLOCKING QUEUE
			socketQueue.add(sock);
		}
	}
	
	public static String getSessionId (ArrayList<String> headerLines) {
		for (String s : headerLines) {
			String sLower = s.toLowerCase();
			if(sLower.contains("cookie: sessionid=")) {
				String sessionid = s.substring(18);
				return sessionid;
			}
		}
		return null;
	}
	
	public static SessionImpl getSession (String sessionId) {
		return sessions.get(sessionId);
	}
	
	public static void addSession (String sessionId, SessionImpl s) {
		sessions.put(sessionId, s);
	}
	
	
	public void run() {
		BlockingQueue<Socket> socketQueue = new LinkedBlockingQueue<Socket>();
		
		Thread httpThread = new Thread(() -> {
				ServerSocket ssock = null;
				try {
					ssock = new ServerSocket(port);
					
					for (int i = 0; i < NUM_WORKERS; i++) { 
						SocketProcessor thread = new SocketProcessor(socketQueue, port, directory);
						thread.start();
					}
					
					serverLoop(ssock, socketQueue);
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		);
		httpThread.start();
		
		
		if (secureServerType) {
			Thread httpsThread = new Thread(() -> {
					try {
						String pwd = "secret";
						KeyStore keyStore = KeyStore.getInstance("JKS");
						keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
						KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
						keyManagerFactory.init(keyStore, pwd.toCharArray());
						SSLContext sslContext = SSLContext.getInstance("TLS");
						sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
						ServerSocketFactory factory = sslContext.getServerSocketFactory();
						ServerSocket serverSocketTLS = factory.createServerSocket(securePortNo);
						for (int i = 0; i < NUM_WORKERS; i++) { 
							SocketProcessor thread = new SocketProcessor(socketQueue, securePortNo, directory);
							thread.start();
						}
						
						serverLoop(serverSocketTLS, socketQueue);
						
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			);
			httpsThread.start();
		}
		
		Thread sessionChecker = new Thread(() -> {
				while (true) {
					Iterator<Map.Entry<String,SessionImpl>> iter = sessions.entrySet().iterator();
					while (iter.hasNext()) {
					    Map.Entry<String,SessionImpl> entry = iter.next();
					    SessionImpl s = entry.getValue();
				    	if (s.lastAccessedTime() + s.getMaxActiveInterval()*1000 <= System.currentTimeMillis()) {
							iter.remove();
						}
					}
					
					
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
//				executors (initialize session expiration scheduler), executors. new schedule thread pool,
			}
		);
		sessionChecker.start();
	}
}
