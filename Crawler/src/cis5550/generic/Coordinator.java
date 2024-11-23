package cis5550.generic;

import static cis5550.webserver.Server.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class Coordinator {
//	Map from id to list of ip, port, and time
	public static Map<String, ArrayList<Object>> workers = new HashMap<String, ArrayList<Object>>();
	
	public static Vector<String> getWorkers() {
		Vector<String> workersString = new Vector<String>();
		
		Iterator<Map.Entry<String,ArrayList<Object>>> iter = workers.entrySet().iterator();
		while (iter.hasNext()) {
		    Map.Entry<String,ArrayList<Object>> entry = iter.next();
		    String id = entry.getKey();
			ArrayList<Object> values = entry.getValue();
			long time = (long) values.get(2);
			if (time + 15000 < System.currentTimeMillis()) {
				iter.remove();
				continue;
			}
			String ipPortCombined = (String) values.get(0)+":"+ (String) values.get(1);
			workersString.add(ipPortCombined);
		}
		
//		for (Map.Entry<String, ArrayList<Object>> entry : workers.entrySet()) {
//			String id = entry.getKey();
//			ArrayList<Object> ipPort = entry.getValue();
//			String ipPortCombined = ipPort.get(0)+":"+ipPort.get(1);
//            workersString.append(id+","+ipPortCombined+"\n");
//        }
		return workersString;
	}
	
	public static String getWorkersString() {
		StringBuilder workersString = new StringBuilder();
		
		Iterator<Map.Entry<String,ArrayList<Object>>> iter = workers.entrySet().iterator();
		while (iter.hasNext()) {
		    Map.Entry<String,ArrayList<Object>> entry = iter.next();
		    String id = entry.getKey();
			ArrayList<Object> values = entry.getValue();
			long time = (long) values.get(2);
			if (time + 15000 < System.currentTimeMillis()) {
				iter.remove();
				continue;
			}
			String ipPortCombined = (String) values.get(0)+":"+ (String) values.get(1);
			workersString.append(id+","+ipPortCombined+"\n");
		}
		
//		for (Map.Entry<String, ArrayList<Object>> entry : workers.entrySet()) {
//			String id = entry.getKey();
//			ArrayList<Object> ipPort = entry.getValue();
//			String ipPortCombined = ipPort.get(0)+":"+ipPort.get(1);
//            workersString.append(id+","+ipPortCombined+"\n");
//        }
		return workersString.toString();
	}
	
	public static String workerTable() {
//		Not using, value, just running to remove stale entries
		String removeStale = getWorkersString();
		
		StringBuilder htmlTable = new StringBuilder();
		htmlTable.append("<table>\n");
		htmlTable.append("<tr>\n");
        htmlTable.append("<th>ID</th>\n");
        htmlTable.append("<th>IP</th>\n");
        htmlTable.append("<th>Port</th>\n");
        htmlTable.append("<th>Link</th>\n");
        htmlTable.append("</tr>\n");
		for (Map.Entry<String, ArrayList<Object>> entry : workers.entrySet()) {
			String id = entry.getKey();
			ArrayList<Object> ipPort = entry.getValue();
            String ip = (String) ipPort.get(0);
            String port = (String) ipPort.get(1);
            htmlTable.append("<tr>\n");
            htmlTable.append("<td>"+id+"</td>\n");
            htmlTable.append("<td>"+ip+"</td>\n");
            htmlTable.append("<td>"+port+"</td>\n");
            htmlTable.append("<td><a href=\""+"http://"+ip+":"+port+"/"+"\">Link</a></td>\n");
            htmlTable.append("</tr>\n");
        }
		htmlTable.append("</table>\n");
		return htmlTable.toString();
	}
	
	public static void registerRoutes() {
		get("/ping", (req,res) -> { 
			String id = req.queryParams("id");
			String port = req.queryParams("port");
			String ip = req.ip();
			
			if (id == null || port == null) {
				res.status(400, "Bad Request");
				return "NO";
			}
			try {
				ArrayList<Object> ipPort = new ArrayList<Object>();
				ipPort.add(ip);
				ipPort.add(port);
				ipPort.add(System.currentTimeMillis());
				workers.put(id, ipPort);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return "OK"; 
		});
		
		get("/workers", (req,res) -> { 
			String workersList = getWorkersString();
			int k = workers.size();
			StringBuilder response = new StringBuilder();
			response.append(Integer.toString(k) + "\n");
			response.append(workersList);
			return response.toString();
		});
	}
}
