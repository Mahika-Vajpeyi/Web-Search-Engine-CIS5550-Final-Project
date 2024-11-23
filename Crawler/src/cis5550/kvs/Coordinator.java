package cis5550.kvs;

//import static cis5550.webserver.Server.get;

import static cis5550.webserver.Server.*;

public class Coordinator extends cis5550.generic.Coordinator{
	public static void main (String[] args) {
        String port = args[0];
        port(Integer.parseInt(port));
        registerRoutes();
        get("/", (req,res) -> {
        	res.header("content-type", "text/html");
        	String output = "<html>\n" + 
        					"<body>\n" +
        					"<h1>KVS Coordinator</h1>\n" +
        					workerTable() +
        					"</body>\n" +
        					"</html>";
        	return output;
//        	return "KVS Coordinator" + workerTable(); 
    	});
    }
	
	
}
