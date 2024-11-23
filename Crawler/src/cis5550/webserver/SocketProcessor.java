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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import cis5550.tools.Logger;

public class SocketProcessor extends Thread{
//	private static final Logger logger = Logger.getLogger(Server.class);
	
	protected BlockingQueue<Socket> socketQueue;
	protected int port;
	protected String directory;
	
	public SocketProcessor(BlockingQueue<Socket> socketQueue, int port, String directory) {
		this.socketQueue = socketQueue;
		this.port = port;
		this.directory = directory;
	}
	
	public void run() {
		while (true) {
			Socket sock = null;
			try {
				sock = socketQueue.take();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
//			System.out.println("Connection from: "+sock.getRemoteSocketAddress());
//			logger.info("Incoming connection from"+sock.getRemoteSocketAddress());
			
			InputStream ipr = null;
			try {
				ipr = sock.getInputStream();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
//			while (true) {
			while (!sock.isClosed()) {
//				READING HEADER
				ByteArrayOutputStream bo = new ByteArrayOutputStream();
				
				int curr = 0;
				byte[] agg = null;
				try {
					while((curr = ipr.read()) != -1) {
						bo.write(curr);
						agg = bo.toByteArray();
						
						if (agg.length > 4 
							&& agg[agg.length - 1] == 10 
							&& agg[agg.length - 2] == 13
							&& agg[agg.length - 3] == 10 
							&& agg[agg.length - 4] == 13) 
						{
							break;
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
//				buffer for comming data
//				
//				while there is something in the buffer:
//					check if there is double crlf:
//						there is one HTTP request and parse it 
//					else keep reading
//				
//				socket.close()
				
				
				
				
//				CHECKS FOR CLIENT DISCONNECT
				if (curr == -1) {
					break;
				}
				
				byte[] headerBytes = agg;
				ArrayList<String> headerLines = new ArrayList<String>();
				BufferedReader headerReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(headerBytes)));
				String current = "";
				try {
					while(((current = headerReader.readLine()) != null) && current.length() > 0) {
						headerLines.add(current);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
								
//				READ BODY
				int contentLength = Server.contentLength(headerLines);
//				System.out.println(contentLength);
				byte[] bodyBytes = null;
				ByteArrayOutputStream boBody = new ByteArrayOutputStream();
				int currBody = 0;
				int idx = 0;
				try {
					while((idx < contentLength && (currBody = ipr.read()) != -1)) {
						boBody.write(currBody);
						idx++;
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				bodyBytes = boBody.toByteArray();
				
//				SEND RESPONSE
				OutputStream opstream = null;
				try {
					opstream = sock.getOutputStream();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				PrintWriter writer = new PrintWriter(opstream); 
				
				int statusCode = Server.headerCheck(headerLines);
				
				if (statusCode != 200 || directory.contains("..")) {
					String response = "";
					if (statusCode == 400) {
						response = "HTTP/1.1 400 Bad Request\r\n"
								+ "Content-Type: text/plain\r\n"
								+ "Server: localhost\r\n"
								+ "Content-Length: " + "400 Bad Request".length() + "\r\n\r\n"
								+ "400 Bad Request\n";
					} else if (statusCode == 405) {
						response = "HTTP/1.1 405 Method Not Allowed\r\n"
								+ "Content-Type: text/plain\r\n"
								+ "Server: localhost\r\n"
								+ "Content-Length: " + "405 Method Not Allowed".length() + "\r\n\r\n"
								+ "405 Method Not Allowed\n";
					} else if (statusCode == 501) {
						response = "HTTP/1.1 501 Not Implemented\r\n"
								+ "Content-Type: text/plain\r\n"
								+ "Server: localhost\r\n"
								+ "Content-Length: " + "501 Not Implemented".length() + "\r\n\r\n"
								+ "501 Not Implemented\n";
					} else if (statusCode == 505) {
						response = "HTTP/1.1 505 HTTP Version Not Supported\r\n"
								+ "Content-Type: text/plain\r\n"
								+ "Server: localhost\r\n"
								+ "Content-Length: " + "505 HTTP Version Not Supported".length() + "\r\n\r\n"
								+ "505 HTTP Version Not Supported\n";
					} else if (statusCode == 403 || directory.contains("..")) {
						response = "HTTP/1.1 403 Forbidden\r\n"
								+ "Content-Type: text/plain\r\n"
								+ "Server: localhost\r\n"
								+ "Content-Length: " + "403 Forbidden".length() + "\r\n\r\n"
								+ "403 Forbidden\n";
					}
					writer.write(response);
					writer.flush();
				}
				
				else {
					String requestMethod = Server.requestMethod(headerLines);
					String filepath = Server.filePath(headerLines.get(0));
					if (Server.routingTable.containsKey(requestMethod)) {
//						Has method, e.g. GET, PUSH, PUT routes
						HashMap<String, Route> routes = Server.routingTable.get(requestMethod);
						Map<String, String> params = null;
						Route r = null;
						for (Map.Entry<String,Route> entry : routes.entrySet()) {
							String path = entry.getKey();
							Map<String, String> result = Server.pathMatch(filepath, path);
							if(result != null) {
								params = result;
								r = entry.getValue();
								break;
							}
						}
						if(params != null && r != null) {
//							Has route for that path
							String requestProtocol = Server.requestProtocol(headerLines);
							Map<String, String> headers = Server.headersParse(headerLines);
							Map<String, String> queryParams = null;
							String sessionId = Server.getSessionId(headerLines);
							if(headers.containsKey("content-type")) {
								if (headers.get("content-type").equals("application/x-www-form-urlencoded")) {
									queryParams = Server.processQueryParams(filepath, new String(bodyBytes));
								} else {
									queryParams = Server.processQueryParams(filepath, null);
								}
							} else {
								queryParams = Server.processQueryParams(filepath, null);
							}
							
							RequestImpl req = new RequestImpl(requestMethod, filepath, requestProtocol, 
									headers, queryParams, params, 
									(InetSocketAddress) sock.getRemoteSocketAddress(), bodyBytes, Server.serverInstance, sessionId);
							ResponseImpl res = new ResponseImpl(opstream);
							
							try {
								Object handleReturn = r.handle(req, res);
								if (req.sessionCreated) {
									if (port == Server.securePortNo) {
										res.header("set-cookie", "SessionID=" + req.sessionId() +"; HttpOnly; SameSite=Lax; Secure");
									} else {
										res.header("set-cookie", "SessionID=" + req.sessionId() +"; HttpOnly; SameSite=Lax");
									}
								}
								if (!res.isCommitted()) {
//									IF ALREADY WRITE() THEN DON'T DO HERE
									
									if(handleReturn != null) {
										res.body(handleReturn.toString());
										res.header("content-type", "text/html");
									}
	//									get body, get length of body
									int bodyLength = res.bodyLength();
									res.header("content-length", String.valueOf(bodyLength));
									res.header("connection", "close");
									
									StringBuilder response = new StringBuilder();
									
									response.append("HTTP/1.1 " + String.valueOf(res.getStatusCode()) + 
													" " + res.getReasonPhrase()+ "\r\n");
	//									response.append("content-type: " + res.getType() + "\r\n");
	//									response.append("server: localhost\r\n");
									for (Map.Entry<String,ArrayList<String>> entry : res.getHeaders().entrySet()) {
										ArrayList<String> value = entry.getValue();
										for (String s: value) {
											response.append(entry.getKey() + ": " + s + "\r\n");
										}
									}

									response.append("\r\n");
//										response.append("Content-Length: " + bodyLength + "\r\n\r\n");
									
									if (bodyLength > 0) {
										String bodyS = new String(res.getBody());
										response.append(bodyS);
//										DataOutputStream bodyStream = new DataOutputStream(opstream);
//										bodyStream.write(res.getBody(), 0, bodyLength);
//										bodyStream.flush();
									}

									writer.write(response.toString());
									writer.flush(); 
									

	//									int bodyLength = res.get
	////									res.header(content len)
								}
//								Already finished responding
//								HashMap<String, ArrayList<String>> asdf = res.getHeaders();
//								for (HashMap.Entry<String, ArrayList<String>> e : asdf.entrySet()) {
//									String k = e.getKey();
//									for (String val : e.getValue()) {
//										System.out.println(k + " " + ": " + val);
//									}
//								}
//								String bodyS = new String(res.getBody());
//								System.out.println(bodyS);
//								break;
//											res.send()
							} catch (Exception e) {
								// TODO Auto-generated catch block
//									500 error
								if (!res.isCommitted()) {
									String response = "HTTP/1.1 500 Internal Server Error\r\n"
											+ "Content-Type: text/plain\r\n"
											+ "Server: localhost\r\n"
											+ "Content-Length: " + "500 Internal Server Error".length() + "\r\n\r\n"
											+ "500 Internal Server Error\n";
									writer.write(response);
									writer.flush();
									e.printStackTrace();
								} else {
									break;
								}
							}
							continue;
						}
					}
					
//					READ FILE
					try {
			//			System.out.println(filepath);
//						System.out.println(directory+filepath);
						File f = new File(directory+filepath);
						if(!f.canRead()) {
							throw new SecurityException();
						}
						FileInputStream fileip = new FileInputStream(f);
						
						String modifiedDate = Server.modifiedSince(headerLines);
						
						if(modifiedDate.equals("NONE")) {
//							Don't have to check for modified date
//							SEND RESPONSE
							String response = "HTTP/1.1 200 OK\r\n"
											+ "Content-Type:" + Server.contentType(headerLines) + "\r\n"
											+ "Server: localhost\r\n"
											+ "Content-Length: " + Long.toString(f.length()) + "\r\n\r\n";
							writer.write(response);
							writer.flush();
							
//							DON'T SEND BODY IF NOT GET METHOD (HEAD REQUEST)
							if(Server.getOrHead(headerLines)) {
								DataOutputStream fileStream = new DataOutputStream(opstream);
								byte[] buffer = new byte[(int) f.length()];
								while (fileip.read(buffer) != -1) {
									fileStream.write(buffer, 0, buffer.length);
									fileStream.flush();
							    }
								fileip.close();
							} 
						} else {
//							Need to check modified date
							long fileModified = f.lastModified();
							SimpleDateFormat formatter = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z", Locale.ENGLISH);
							java.util.Date requestedModified = formatter.parse(modifiedDate);
							long requestedModifiedTime = requestedModified.getTime();
							if (requestedModifiedTime < fileModified) {
								String response = "HTTP/1.1 200 OK\r\n"
										+ "Content-Type:" + Server.contentType(headerLines) + "\r\n"
										+ "Server: localhost\r\n"
										+ "Content-Length: " + Long.toString(f.length()) + "\r\n\r\n";
								writer.write(response);
								writer.flush();
								
		//						DON'T SEND BODY IF NOT GET METHOD (HEAD REQUEST)
								if(Server.getOrHead(headerLines)) {
									DataOutputStream fileStream = new DataOutputStream(opstream);
									byte[] buffer = new byte[(int) f.length()];
									while (fileip.read(buffer) != -1) {
										fileStream.write(buffer, 0, buffer.length);
										fileStream.flush();
								    }
									fileip.close();
								} 
							} else {
//								Not modified since, so error
								String response = "HTTP/1.1 304 Not Modified\r\n"
										+ "Content-Type: text/plain\r\n"
										+ "Server: localhost\r\n"
										+ "Content-Length: " + "304 Not Modified".length() + "\r\n\r\n"
										+ "304 Not Modified\n";
								writer.write(response);
								writer.flush();
							}
						}
						
//					System.out.println(Arrays.toString(buffer));
					} catch (FileNotFoundException e) {
						String response = "HTTP/1.1 404 Not Found\r\n"
								+ "Content-Type: text/plain\r\n"
								+ "Server: localhost\r\n"
								+ "Content-Length: " + "404 Not Found".length() + "\r\n\r\n"
								+ "404 Not Found\n";
						writer.write(response);
						writer.flush();
					} catch (SecurityException e) {
						String response = "HTTP/1.1 403 Forbidden\r\n"
								+ "Content-Type: text/plain\r\n"
								+ "Server: localhost\r\n"
								+ "Content-Length: " + "403 Forbidden".length() + "\r\n\r\n"
								+ "403 Forbidden\n";
						writer.write(response);
						writer.flush();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				
//				ALSO CHECKS FOR DISCONNECT
				if (Server.closeConnection(headerLines)) {
					break;
				}
			}
			try {
				sock.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
