package cis5550.generic;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.kvs.Row;
import cis5550.tools.HTTP;

public class Worker {
	public static Map<String, Map<String, Row>> tables = new ConcurrentHashMap<String, Map<String, Row>>();
	
	public static void startPingThread(String coordIpPort, String workerPort, String id) {
		Thread pingThread = new Thread(() -> {
			while (true) {				
				String url = "http://"+coordIpPort+"/ping?id="+id+"&port="+workerPort;
				try {
					HTTP.doRequest("GET", url, null);
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		pingThread.start();
	}
	
	public static String allTablesTable(String directory) {
//		Not using, value, just running to remove stale entries		
		StringBuilder htmlTable = new StringBuilder();
		htmlTable.append("<table>\n");
		htmlTable.append("<tr>\n");
        htmlTable.append("<th>Name</th>\n");
        htmlTable.append("<th>Number of Keys</th>\n");
        htmlTable.append("</tr>\n");
		for (Map.Entry<String, Map<String, Row>> entry : tables.entrySet()) {
			String name = entry.getKey();
			int keyCount = entry.getValue().size();
            htmlTable.append("<tr>\n");
            htmlTable.append("<td><a href=\""+"/view/"+name+"\">"+name+"</a></td>\n");
            htmlTable.append("<td>"+String.valueOf(keyCount)+"</td>\n");
            htmlTable.append("</tr>\n");
        }
		File dirFile = new File(directory);
		File[] tables = dirFile.listFiles(File::isDirectory);
		for (File table : tables) {
			File[] rows = table.listFiles();
			htmlTable.append("<tr>\n");
            htmlTable.append("<td><a href=\""+"/view/"+table.getName()+"\">"+table.getName()+"</a></td>\n");
            htmlTable.append("<td>"+String.valueOf(rows.length)+"</td>\n");
            htmlTable.append("</tr>\n");
		}
		htmlTable.append("</table>\n");
		return htmlTable.toString();
	}
	
	public static String singleTablesTable(String name, String directory, String fromRow) {
//		Not using, value, just running to remove stale entries	
		SortedSet<String> columns = new TreeSet<String>();
		if (name.length() >= 3 && name.substring(0,3).equals("pt-")) {
			String dirPath = directory+"/"+name;
			File dirFile = new File(dirPath);
			File[] rows = dirFile.listFiles();
//			File has compareTo based on name so this is sorting based on file (row) name
			Arrays.sort(rows);
			List<File> rowsList= Arrays.asList(rows);
			try {
				int i = 0;
				for (File row: rows) {
//					rowKey here is based on encoded (file name) as sorted on that
					String rowKey = row.getName();
					if(rowKey.compareTo(fromRow)>=0) {
						i++;
						FileInputStream fileip = new FileInputStream(row);
						Row r = Row.readFrom(fileip);
			            Set<String> colSet = r.columns();
			            columns.addAll(colSet);
					}
					if (i == 10) {
						break;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			StringBuilder htmlTable = new StringBuilder();
			htmlTable.append("<table>\n");
			htmlTable.append("<tr>\n");
			htmlTable.append("<th>Row Key</th>\n");
			for (String s : columns) {
				htmlTable.append("<th>"+s+"</th>\n");
			}
	        htmlTable.append("</tr>\n");
        	Iterator<File> iterator = rowsList.iterator();
        	int rowCount = 0;
	        try {
		        while (iterator.hasNext() && rowCount < 10) {
		        	File row = iterator.next();
		        	String rowKey = row.getName();
		        	if(rowKey.compareTo(fromRow)>=0) {
		        		FileInputStream fileip = new FileInputStream(row);
						Row r = Row.readFrom(fileip);
			            htmlTable.append("<tr>\n");
//			            r.key() here is unencoded
			            htmlTable.append("<td>"+r.key()+"</td>\n");
			            for (String s : columns) {
			    			if(r.get(s) != null) {
			    				htmlTable.append("<td>"+r.get(s)+"</td>\n");
			    			} else {
			    				htmlTable.append("<td></td>\n");
			    			}
			    		}
			            htmlTable.append("</tr>\n");
			            rowCount++;
		        	}
		        }
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }
			htmlTable.append("</table>\n");
            if (rowCount == 10 && iterator.hasNext()) {
//            	Reached end of current 10, check if there's any more in which case add for button.
//            	If not at multiple of 10 and reached end, won't run this so won't need button
        		File nextRow = iterator.next();
//        		getName() here is encoded as sorting on encoded name (but displaying unencoded)
        		htmlTable.append("<a href=\""+"/view/"+name+"?fromRow="+nextRow.getName()+"\">Next</a>\n");
            }
			
			return htmlTable.toString();
			
		} else {
//			Will be sorted by key as using ConcurrentSkipListMap
			Map<String, Row> table = tables.get(name);
			int i = 0;
			for (Map.Entry<String, Row> entry : table.entrySet()) {
				String rowKey = entry.getKey();
				if(rowKey.compareTo(fromRow)>=0) {
					i++;
					Row r = entry.getValue();
		            Set<String> colSet = r.columns();
		            columns.addAll(colSet);
				}
				if (i == 10) {
					break;
				}
	        }
			
			StringBuilder htmlTable = new StringBuilder();
			htmlTable.append("<table>\n");
			htmlTable.append("<tr>\n");
			htmlTable.append("<th>Row Key</th>\n");
			for (String s : columns) {
				htmlTable.append("<th>"+s+"</th>\n");
			}
	        htmlTable.append("</tr>\n");
	        
	        int rowCount = 0;
	        Iterator<Map.Entry<String, Row>> iterator = table.entrySet().iterator();
	        while (iterator.hasNext() && rowCount < 10) {
	        	Map.Entry<String, Row> entry = iterator.next();
				String rowKey = entry.getKey();
				if (rowKey.compareTo(fromRow)>=0) {
					Row r = entry.getValue();
		            htmlTable.append("<tr>\n");
		            htmlTable.append("<td>"+rowKey+"</td>\n");
		            for (String s : columns) {
		    			if(r.get(s) != null) {
		    				htmlTable.append("<td>"+r.get(s)+"</td>\n");
		    			} else {
		    				htmlTable.append("<td></td>\n");
		    			}
		    		}
		            htmlTable.append("</tr>\n");
		            rowCount ++;
				}
	        }
			htmlTable.append("</table>\n");
            if (rowCount == 10 && iterator.hasNext()) {
//            	Reached end of current 10, check if there's any more in which case add for button.
//            	If not at multiple of 10 and reached end, won't run this so won't need button
        		Map.Entry<String, Row> nextRow = iterator.next();
        		htmlTable.append("<a href=\""+"/view/"+name+"?fromRow="+nextRow.getKey()+"\">Next</a>\n");
            }
			return htmlTable.toString();
		}
	}
}
