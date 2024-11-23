package cis5550.flame;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToBoolean;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameRDD.StringToPairIterable;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker {
	
    public static final char[] ALLOWED_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray();
    public static final char[] ALLOWED_FIRST_CHARACTERS = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    
    public static String generateRandomString(int length) {
        Random random = new Random();
        char[] randomChars = new char[length];
        randomChars[0] = ALLOWED_FIRST_CHARACTERS[random.nextInt(ALLOWED_FIRST_CHARACTERS.length)];
        for (int i = 1; i < length; i++) {
            randomChars[i] = ALLOWED_CHARACTERS[random.nextInt(ALLOWED_CHARACTERS.length)];
        }
        return new String(randomChars);
    }

	public static String[] decodeQueryParams (Request request) {
        String[] result = new String[8];
        result[0] = request.queryParams("input");
        result[1] = request.queryParams("output");
        result[2] = request.queryParams("coordinator");
        result[3] = request.queryParams("low");
        result[4] = request.queryParams("high");
        result[5] = request.queryParams("zeroElement");
        result[6] = request.queryParams("prob");
        result[7] = request.queryParams("other");
        return result;
	}
	
	public static void main(String args[]) {
    if (args.length != 2) {
    	System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
    	System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
	  startPingThread(server, ""+port, String.valueOf(port));
    final File myJAR = new File("__worker"+port+"-current.jar");
    

  	port(port);

    post("/useJAR", (request,response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });
    
    post("/rdd/flatMap", (request,response) -> {
    	String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
        String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        FlameRDD.StringToIterable lambda = (StringToIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        int consecTimeouts = 0;
        while (rows.hasNext()) {
        	Row r = rows.next();
            Future<Iterable<String>> future = executor.submit(() -> lambda.op(r.get("value")));
            try {
                Iterable<String> res = future.get(5, TimeUnit.SECONDS);
                if (res != null) {
                    Iterator<String> it = res.iterator();
                    while (it.hasNext()) {
                        String val = it.next();
                        try {
                            kvs.put(outputTable, generateRandomString(32), "value", val);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                consecTimeouts = 0;
            } catch (Exception e) {
                // Handle timeout
            	consecTimeouts++;
                e.printStackTrace();
                if(consecTimeouts >= 5) {
                	System.out.println("CONSECUTIVE TIMEOUTS");
                	break;
                }
            } finally {
                // Cancel the task if it exceeds the timeout
                future.cancel(true);
            }
        }
        executor.shutdown();
//        	Row r = rows.next();
//        	Iterable<String> res = lambda.op(r.get("value"));
//        	if (res != null) {
//        		Iterator<String> it = res.iterator();
//        		while (it.hasNext()) {
//        			String val = it.next();
//    				try {
//    					kvs.put(outputTable, UUID.randomUUID().toString(), "value", val);
//    				} catch (Exception e) {
//    					e.printStackTrace();
//    				}
//        		}
//        	}
        return "OK";
      });
	
	post("/rdd/mapToPair", (request,response) -> {
    	String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
        String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        FlameRDD.StringToPair lambda = (StringToPair) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
        while (rows.hasNext()) {
        	Row r = rows.next();
        	FlamePair res = lambda.op(r.get("value"));
        	if (res != null) {
            	String k = res._1();
            	String v = res._2();
				try {
					kvs.put(outputTable, k, r.key(), v);
				} catch (Exception e) {
					e.printStackTrace();
				}
        	}
        }
        return "OK";
      });
	
	post("/rddPair/foldByKey", (request,response) -> {
    	String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
        String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        String zeroElement = URLDecoder.decode(queryParamsDecoded[5], "UTF-8");
        FlamePairRDD.TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
        while (rows.hasNext()) {
        	Row r = rows.next();
        	String acc = zeroElement;
        	Set<String> columns = r.columns();
			for (String c : columns) {
				String v = r.get(c);
				String newAcc = lambda.op(acc, v);
				acc = newAcc;
			}
			try {
				kvs.put(outputTable, r.key(), UUID.randomUUID().toString(), acc);
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
        return "OK";
      });
	
	post("/rdd/sampling", (request,response) -> {
    	String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
        String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        double prob = Double.parseDouble(queryParamsDecoded[6]);
        
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
        while (rows.hasNext()) {
        	Row r = rows.next();
        	Random rand = new Random();
        	double generatedProb = rand.nextDouble();
        	if (generatedProb <= prob) {
        		try {
    				kvs.put(outputTable, r.key(), "value", r.get("value"));
    			} catch (Exception e) {
    				e.printStackTrace();
    			}
        	}
        }
        return "OK";
      });
	
	post("/rdd/fromTable", (request,response) -> {
    	String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
        String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        FlameContext.RowToString lambda = (RowToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
        while (rows.hasNext()) {
        	Row r = rows.next();
        	String res = lambda.op(r);
        	if (res != null) {
        		kvs.put(outputTable, r.key(), "value", res);
        	}
        }
        return "OK";
      });
	
	post("/rdd/flatMapToPair", (request,response) -> {
    	String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
        String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        FlameRDD.StringToPairIterable lambda = (StringToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
        while (rows.hasNext()) {
        	Row r = rows.next();
        	Iterable<FlamePair> res = lambda.op(r.get("value"));
        	if (res != null) {
        		Iterator<FlamePair> it = res.iterator();
        		int i = 0;
        		while (it.hasNext()) {
        			FlamePair val = it.next();
    				try {
    					kvs.put(outputTable, val._1(), Hasher.hash(r.key() + Integer.toString(i)), val._2());
    					i++;
    				} catch (Exception e) {
    					e.printStackTrace();
    				}
        		}
        	}
        }
        return "OK";
      });
	
	post("/rddPair/flatMap", (request,response) -> {
    	String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
        String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        FlamePairRDD.PairToStringIterable lambda = (PairToStringIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
        while (rows.hasNext()) {
        	Row r = rows.next();
        	Set<String> columns = r.columns();
			for (String c : columns) {
				String v = r.get(c);
				FlamePair pair = new FlamePair(r.key(), v);
				Iterable<String> res = lambda.op(pair);
				if (res != null) {
					for (String s : res) {
						kvs.put(outputTable, UUID.randomUUID().toString(), "value", s);
					}
				}
			}
        }
        return "OK";
      });
	
	post("/rddPair/flatMapToPair", (request,response) -> {
    	String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
        String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        FlamePairRDD.PairToPairIterable lambda = (PairToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
        while (rows.hasNext()) {
        	Row r = rows.next();
        	Set<String> columns = r.columns();
			for (String c : columns) {
				String v = r.get(c);
				FlamePair pair = new FlamePair(r.key(), v);
				Iterable<FlamePair> res = lambda.op(pair);
				for (FlamePair fp : res) {
					kvs.put(outputTable, fp._1(), UUID.randomUUID().toString(), fp._2());
				}
			}
        }
        return "OK";
      });
	
	post("/rdd/distinct", (request,response) -> {
    	String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
        String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
        while (rows.hasNext()) {
        	Row r = rows.next();
        	String v = r.get("value");
			kvs.put(outputTable, v, "value", v);
        }
        return "OK";
      });
	
	post("/rddPair/join", (request,response) -> {
    	String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
        String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        String other = queryParamsDecoded[7];
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
        while (rows.hasNext()) {
        	Row row = rows.next();
        	Row otherRow = kvs.getRow(other, row.key());
        	if (otherRow != null) {
        		Set<String> columnsOne = row.columns();
        		Set<String> columnsTwo = otherRow.columns();
    			for (String cOne : columnsOne) {
    				String vOne = row.get(cOne);
    				for (String cTwo : columnsTwo) {
        				String vTwo = otherRow.get(cTwo);
        				kvs.put(outputTable, row.key(), Hasher.hash(cOne + "~" + cTwo), vOne+","+vTwo);
        			}
    			}
        	}
        }
        return "OK";
      });
	
	post("/rdd/fold", (request,response) -> {
		String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
    	String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        String zeroElement = URLDecoder.decode(queryParamsDecoded[5], "UTF-8");
        FlamePairRDD.TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
    	String acc = zeroElement;
        while (rows.hasNext()) {
        	Row r = rows.next();
        	String v = r.get("value");
			acc = lambda.op(acc, v);
        }
        kvs.put(outputTable, UUID.randomUUID().toString(), "value", acc);
        return "OK";
      });
	
	post("/rdd/filter", (request,response) -> {
		String[] queryParamsDecoded = decodeQueryParams(request);
    	String inputTable = queryParamsDecoded[0];
        String outputTable = queryParamsDecoded[1];
        String KVSCoordinator = queryParamsDecoded[2];
        String low = queryParamsDecoded[3];
        String high = queryParamsDecoded[4];
        FlameRDD.StringToBoolean lambda = (StringToBoolean) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        KVSClient kvs = new KVSClient(KVSCoordinator);
        Iterator<Row> rows = kvs.scan(inputTable, low, high);
        while (rows.hasNext()) {
        	Row r = rows.next();
        	String v = r.get("value");
        	if (lambda.op(v)) {
    			kvs.put(outputTable, r.key(), "value", v);
        	}
        }
        return "OK";
      });
	
	}	
}
