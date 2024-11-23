package cis5550.kvs;

import static cis5550.webserver.Server.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import cis5550.jobs.ProcessQuery;

public class Worker extends cis5550.generic.Worker {
	public static void putRow(String t, Row r) {
		tables.get(t).put(r.key(),r);
	}
	
	public static Row getRow(String t, String k) {
		return tables.get(t).get(k);
	}
	
	public static void main (String[] args) {
        String workerPort = args[0];
        String directory = args[1];
        String coordIpPort = args[2];
        
        String id = "";
                
        try {
        	File f = new File(directory+"/id");
        	if (f.exists()) {
        		FileInputStream fileip = new FileInputStream(f);
            	byte[] buffer = new byte[(int) f.length()];
    			fileip.read(buffer);
    			id = new String(buffer);
    			fileip.close();
        	}
        	else {
        		StringBuilder randomString = new StringBuilder();
				Random random = new Random();
		        for (int i = 0; i < 5; i++) {
		            int randomNumber = random.nextInt(26) + 97;
		            char randomChar = (char) randomNumber;
		            randomString.append(randomChar);
		        }
		        id = randomString.toString();
		        FileOutputStream outputStream = new FileOutputStream(f);
		        byte[] idBytes = id.getBytes();
		        outputStream.write(idBytes);
		        outputStream.close();
        	}
        } catch (Exception e) {
        	e.printStackTrace();
        }
        
        port(Integer.parseInt(workerPort));
        startPingThread(coordIpPort, workerPort, id);
        
        get("/search", (req, res) -> {
        	res.header("Access-Control-Allow-Origin", "*");
        	String searchTerm = req.queryParams("query");
        	System.out.println("query " + searchTerm);
        	
        	res.status(200, "OK");
        	//KVSClient kvs = new KVSClient("23.123.43.2:8000");
        	KVSClient kvs = new KVSClient("localhost:8000");
        	
        	ProcessQuery pq = new ProcessQuery(kvs);
        	String results = pq.returnResults(searchTerm);
        	
//        	Row r = getRow("pt-cache", searchTerm);
//        	String results = r.get("1");
        	
//        	System.out.println("results " + results);
        	

        	res.type("application/json"); 
        	//return "{\"result\": \"Test data from server\"}";
        	return results;

        });
        
        put("/data/:t/:r/:c", (req,res) -> { 
        	String table = req.params("t");
        	String row = URLDecoder.decode(req.params("r"));
        	String column = URLDecoder.decode(req.params("c"));
        	
//        	Extra credit
        	if(req.queryParams("ifcolumn") != null && req.queryParams("equals") != null) {
        		String colName = req.queryParams("ifcolumn");
        		String colValue = req.queryParams("equals");
        		if(!tables.containsKey(table)) {
        			return "FAIL";
        		} else if (!tables.get(table).containsKey(row)) {
        			return "FAIL";
        		} else if (tables.get(table).get(row).get(colName) == null) {
        			return "FAIL";
        		} else if (tables.get(table).get(row).get(colName) != colValue) {
        			return "FAIL";
        		}
        	}
        	
        	if (table.length() >= 3 && table.substring(0,3).equals("pt-")) {
//        		Persistent
        		String dirPath = directory+"/"+table;
        		String rowEncoded = cis5550.tools.KeyEncoder.encode(row);
        		File directoryFile = new File(dirPath);
        		File fileFile = new File(dirPath + "/" + rowEncoded);
        		
//        		Make directory and row file for table if not already exists
        		Row r = new Row(row);
        		try {
        			if(!directoryFile.exists()) {
        				directoryFile.mkdirs();
        			}
        			if(!fileFile.exists()) {
        				fileFile.createNewFile();
        			} else {
//        				File already exists
        				FileInputStream fileip = new FileInputStream(fileFile);
        				r = Row.readFrom(fileip);
        			}
        		} catch (Exception e) {
        			e.printStackTrace();
        		}
        		
//        		Row r is now row of either new or existing row file
        		r.put(column, req.bodyAsBytes());
        		byte[] dataOutput = r.toByteArray();
        		FileOutputStream outputStream = new FileOutputStream(fileFile);
		        outputStream.write(dataOutput);
		        outputStream.close();            	
        	} else {
        		if(tables.containsKey(table)) {
    				Map<String, Row> existing = tables.get(table);
    				if(existing.containsKey(row)) {
    					byte[] data = req.bodyAsBytes();
    					Row currRow = existing.get(row);
    					currRow.put(column, data);
    					putRow(table, currRow);
    				} else {
    					byte[] data = req.bodyAsBytes();
    					Row newRow = new Row(row);
    					newRow.put(column, data);
    					putRow(table, newRow);
    				}
    			} else {
    				tables.put(table, new ConcurrentSkipListMap<String, Row>());
    				Map<String, Row> newTable = tables.get(table);
    				byte[] data = req.bodyAsBytes();
    				Row newRow = new Row(row);
    				newRow.put(column, data);
    				putRow(table, newRow);
    			}
        	}
        	
			return "OK";
		});
        
        put("/data/:t", (req,res) -> { 
        	String table = req.params("t");
        	ByteArrayInputStream reqBody = new ByteArrayInputStream(req.bodyAsBytes());
        	while(true) {
                Row rowData = Row.readFrom((InputStream)reqBody);
                if (rowData == null) {
                   return "OK";
                }
                if (table.length() >= 3 && table.substring(0,3).equals("pt-")) {
//            		Persistent
            		String dirPath = directory+"/"+table;
            		String rowEncoded = cis5550.tools.KeyEncoder.encode(rowData.key());
            		File directoryFile = new File(dirPath);
            		File fileFile = new File(dirPath + "/" + rowEncoded);
            		
//            		Make directory and row file for table if not already exists
            		Row r = rowData;

            		try {
            			if(!directoryFile.exists()) {
            				directoryFile.mkdirs();
            			}
            			if(!fileFile.exists()) {
            				fileFile.createNewFile();
            			}
            		} catch (Exception e) {
            			e.printStackTrace();
            		}
            		
//            		Row r is now row of either new or existing row file
            		byte[] dataOutput = r.toByteArray();
            		FileOutputStream outputStream = new FileOutputStream(fileFile, false);
    		        outputStream.write(dataOutput);
    		        outputStream.close();            	
            	} else {
            		if(tables.containsKey(table)) {
        				putRow(table, rowData);
        			} else {
        				tables.put(table, new ConcurrentSkipListMap<String, Row>());
        				putRow(table, rowData);
        			}
            	}
             }
        });
        
        get("/data/:t/:r/:c", (req,res) -> { 
        	String table = req.params("t");
        	String row = req.params("r");
        	String column = req.params("c");
        	
//        	Persistent table
        	if (table.length() >= 3 && table.substring(0,3).equals("pt-")) {
        		String dirPath = directory+"/"+table;
        		String rowEncoded = cis5550.tools.KeyEncoder.encode(row);
        		File fileFile = new File(dirPath + "/" + rowEncoded);
        		if (fileFile.exists()) {
        			FileInputStream fileip = new FileInputStream(fileFile);
    				Row r = Row.readFrom(fileip);
    				if (r.get(column) == null) {
    					res.status(404, "Not Found");
    				} else {
    					res.bodyAsBytes(r.getBytes(column));
    				}
        		} else {
        			res.status(404, "Not Found");
        		}
        		return null;
        	}
        	
        	else {
        		if (tables.containsKey(table) && tables.get(table).containsKey(row) && tables.get(table).get(row).get(column) != null) {
            		byte[] data = getRow(table, row).getBytes(column);
            		res.bodyAsBytes(data);
            	} else {
            		res.status(404, "Not Found");
            	}
            	return null;
        	}
		});
        
        get("/", (req,res) -> {     		
        	res.header("content-type", "text/html");
        	String output = "<html>\n" + 
        					"<body>\n" +
        					"<h1>Tables</h1>\n" +
        					allTablesTable(directory) +
        					"</body>\n" +
        					"</html>";
        	return output;
//        	return "KVS Coordinator" + workerTable(); 
		});
        
        get("/view/:table", (req,res) -> {  
        	String table = req.params("table");
        	String fromRow = req.queryParams("fromRow");
        	if (fromRow == null) {
        		fromRow = "";
        	}
        	res.header("content-type", "text/html");
        	String output = "<html>\n" + 
        					"<body>\n" +
        					"<h1>"+table+"</h1>\n" +
        					singleTablesTable(table, directory, fromRow) +
        					"</body>\n" +
        					"</html>";
        	return output;
		});
        
        get("/data/:table/:row", (req,res) -> {  
        	String table = req.params("table");
        	String row = req.params("row");
        	if (table.length() >= 3 && table.substring(0,3).equals("pt-")) {
        		String dirPath = directory+"/"+table;
        		String rowEncoded = cis5550.tools.KeyEncoder.encode(row);
        		File fileFile = new File(dirPath + "/" + rowEncoded);
        		if (fileFile.exists()) {
        			FileInputStream fileip = new FileInputStream(fileFile);
    				Row r = Row.readFrom(fileip);
					res.bodyAsBytes(r.toByteArray());
        		} else {
        			res.status(404, "Not Found");
        		}
        		return null;
        	}
        	
        	else {
        		if (tables.containsKey(table) && tables.get(table).containsKey(row)) {
            		byte[] data = getRow(table, row).toByteArray();
            		res.bodyAsBytes(data);
            	} else {
            		res.status(404, "Not Found");
            	}
            	return null;
        	}
		});
        
        get("/data/:table", (req,res) -> {  
        	String table = req.params("table");
        	String startRow = req.queryParams("startRow");
        	String endRowExclusive = req.queryParams("endRowExclusive");
        	
        	if (table.length() >= 3 && table.substring(0,3).equals("pt-")) {
        		String dirPath = directory+"/"+table;
        		File dirFile = new File(dirPath);
        		if (dirFile.exists()) {
        			File[] rows = dirFile.listFiles();
        			res.header("content-type", "text/plain");
        			if (rows != null) {
        				for (File row : rows) {
        					FileInputStream fileip = new FileInputStream(row);
            				Row r = Row.readFrom(fileip);
            				String rowKey = r.key();
            				if ((startRow == null || startRow.compareTo(rowKey) <= 0) && (endRowExclusive == null || endRowExclusive.compareTo(rowKey) > 0)) {
            					byte[] data = r.toByteArray();
            					res.write(data);
            					res.write("\n".getBytes());
            				}
        				}
        			}
        			res.write("\n".getBytes());
        		} else {
        			res.status(404, "Not Found");
        		}
        		return null;
        	}
        	
        	else {
        		if (tables.containsKey(table)) {
        			res.header("content-type", "text/plain");
        			for (Map.Entry<String, Row> entry : tables.get(table).entrySet()) {
        				String rowKey = entry.getKey();
        				Row r = entry.getValue();
        				if ((startRow == null || startRow.compareTo(rowKey) <= 0) && (endRowExclusive == null || endRowExclusive.compareTo(rowKey) > 0)) {
        					byte[] data = r.toByteArray();
//        					String dataS = new String (data);
//        					System.out.println(dataS);
        					res.write(data);
        					res.write("\n".getBytes());
        				}
        	        }
        			res.write("\n".getBytes());
            	} else {
            		res.status(404, "Not Found");
            	}
            	return null;
        	}
		});
        
        get("/count/:table", (req,res) -> {  
        	String table = req.params("table");
        	if (table.length() >= 3 && table.substring(0,3).equals("pt-")) {
        		String dirPath = directory+"/"+table;
        		File dirFile = new File(dirPath);
        		if (dirFile.exists()) {
        			File[] rows = dirFile.listFiles();
					res.body(String.valueOf(rows.length));
        		} else {
        			res.status(404, "Not Found");
        		}
        		return null;
        	}
        	
        	else {
        		if (tables.containsKey(table)) {
        			Map<String, Row> entries = tables.get(table);
            		res.body(String.valueOf(entries.size()));
            	} else {
            		res.status(404, "Not Found");
            	}
            	return null;
        	}
		});
        
        put("/rename/:table", (req,res) -> {  
        	String newName = req.body();
        	String table = req.params("table");

        	if ((table.length() >= 3 && table.substring(0,3).equals("pt-")) && (newName.length() < 3 || !newName.substring(0,3).equals("pt-"))) {
        		res.status(400, "Bad Request");
        		return null;
        	}
        	if ((table.length() >= 3 && !table.substring(0,3).equals("pt-")) && (newName.length() >= 3 && newName.substring(0,3).equals("pt-"))){
        		for (Map.Entry<String, Row> entry : tables.get(table).entrySet()) {
    				String rowKey = entry.getKey();
    				Row r = entry.getValue();
    				String dirPath = directory+"/"+newName;
            		String rowEncoded = cis5550.tools.KeyEncoder.encode(rowKey);
            		File directoryFile = new File(dirPath);
            		File fileFile = new File(dirPath + "/" + rowEncoded);
            		
//            		Make directory and row file for table if not already exists
            		try {
            			if(!directoryFile.exists()) {
            				directoryFile.mkdirs();
            			}
            			if(!fileFile.exists()) {
            				fileFile.createNewFile();
            			} else {
//            				File already exists
            				FileInputStream fileip = new FileInputStream(fileFile);
            				r = Row.readFrom(fileip);
            			}
            		} catch (Exception e) {
            			e.printStackTrace();
            		}
            		
            		byte[] dataOutput = r.toByteArray();
            		FileOutputStream outputStream = new FileOutputStream(fileFile);
    		        outputStream.write(dataOutput);
    		        outputStream.close();     
    	        }
        		return "OK";
        	}

        	if (table.length() >= 3 && table.substring(0,3).equals("pt-")) {
        		String dirPath = directory+"/"+table;
        		File dirFile = new File(dirPath);
        		if (dirFile.exists()) {        			
        			try {
            			Path oldDirPath = Paths.get(dirPath);
            			Path newDirPath = oldDirPath.resolveSibling(newName);
            			Files.move(oldDirPath, newDirPath);
            		} catch (FileAlreadyExistsException e) {
            			res.status(409, "Conflict");
            			return null;
            		} catch (Exception e) {
            			e.printStackTrace();
            		}
        		} else {
        			res.status(404, "Not Found");
        			return null;
        		}
        		return "OK";
        	}
        	
        	else {
        		if (tables.containsKey(table)) {
        			if (tables.containsKey(newName)) {
        				res.status(409, "Conflict");
            			return null;
        			} else {
        				Map<String, Row> data = tables.remove(table);
        				tables.put(newName, data);
        			}
            	} else {
            		res.status(404, "Not Found");
            		return null;
            	}
            	return "OK";
        	}
		});
        
        put("/delete/:table", (req,res) -> {  
        	String table = req.params("table");
        	if (table.length() >= 3 && table.substring(0,3).equals("pt-")) {
        		String dirPath = directory+"/"+table;
        		File dirFile = new File(dirPath);
        		if (dirFile.exists()) {        			
        			File[] rows = dirFile.listFiles();
        			for (File r : rows) {
        				r.delete();
        			}
        			dirFile.delete();
        		} else {
        			res.status(404, "Not Found");
        			return null;
        		}
        		return "OK";
        	}
        	
        	else {
        		if (tables.containsKey(table)) {
        			tables.remove(table);
            	} else {
            		res.status(404, "Not Found");
            		return null;
            	}
            	return "OK";
        	}
		});
        
        get("/tables", (req,res) -> {  
        	res.header("content-type", "text/plain");
        	StringBuilder tablesString = new StringBuilder();
        	for (Map.Entry<String, Map<String, Row>> entry : tables.entrySet()) {
    			String name = entry.getKey();
    			tablesString.append(name+"\n");
            }
        	File dirFile = new File(directory);
    		File[] tables = dirFile.listFiles(File::isDirectory);
    		for (File table : tables) {
    			File[] rows = table.listFiles();
    			String name = table.getName();
    			tablesString.append(name+"\n");
    		}
    		return tablesString.toString();
		});
    }
}
