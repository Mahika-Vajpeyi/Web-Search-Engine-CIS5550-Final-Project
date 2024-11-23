package cis5550.jobs;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.external.PorterStemmer;
import cis5550.flame.FlameContext;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;




public class ProcessQuery {
	
	private static String jsonOutput;
	private String searchTerm;
	private static KVSClient kvs;
	
	public ProcessQuery(KVSClient kvs) {
		this.kvs = kvs;
		
		System.out.println("search term in process query " + searchTerm);
		
	}
	
//	public String returnResults() {
//		
//		return jsonOutput;
//	}
	
	private static String stem(String input) {
		PorterStemmer stemmer = new PorterStemmer();
		stemmer.add(input.toCharArray(), input.length());
	    stemmer.stem();
	    return stemmer.toString();

    }
	
	public String returnResults(String query) throws Exception{
		
		ConcurrentHashMap<String, Double> queryTF_map = new ConcurrentHashMap<>();
		ConcurrentHashMap<String, Double> queryTF_IDF_map = new ConcurrentHashMap<>();
		//String query = args[0];
		System.out.println("query: " + query);
		
		String[] queryParts = query.split(" ");
		
		for(int i = 0; i < queryParts.length; i++) {
            String s = queryParts[i];
            s = s.replaceAll("[^a-zA-Z ]", " ").toLowerCase().trim();
            queryParts[i] = stem(s);
            s = stem(s);
            System.out.println("stemmed s " + s);
            double freq = queryTF_map.getOrDefault(s, 0.0);
            queryTF_map.put(s, freq+1);
        }
//		
		for (Map.Entry<String, Double> entry : queryTF_map.entrySet()) {
			System.out.println("***** query freq*****");
            System.out.println("Word: " + entry.getKey() + ", Frequency: " + entry.getValue());
        }
		
		for(Map.Entry<String, Double> e: queryTF_map.entrySet()) {
			System.out.println("map val " + e.getValue() + " as double " + e.getValue()*1.0 + " len " + query.length());
			queryTF_map.put(e.getKey(), (e.getValue()*1.0)/queryParts.length);
		}
		
		for (Map.Entry<String, Double> entry : queryTF_map.entrySet()) {
			System.out.println("***** query tf*****");
            System.out.println("Word: " + entry.getKey() + ", Frequency: " + entry.getValue());
        }
		
		
		
		//KVSClient k = context.getKVS();
		int numDocs = kvs.count("pt-crawl");
		
		
		ConcurrentHashMap<String, String> computedVals = new ConcurrentHashMap();
		
		double queryIDF = 0.0;
		double queryTF_IDF = 0.0;
		double docWeight = 0.0;
		
		for(int i=0; i<queryParts.length; i++) {
			
			//TF-IDF Calculation
			if(kvs.existsRow("pt-computed", queryParts[i])) 
			{

				Row r = kvs.getRow("pt-computed", queryParts[i]);
				Set<String> cols = r.columns();
				int numDocsWithTerm = cols.size();
				int j = 0;
				
				queryIDF = Math.log((numDocs)*1.0/numDocsWithTerm);
			    System.out.println("queryIDF " + queryIDF);
			    System.out.println("queryTF.get(queryParts[i]) " + queryTF_map.get(queryParts[i]));
		    	queryTF_IDF = queryTF_map.get(queryParts[i]) * queryIDF;
		    	queryTF_IDF_map.put(queryParts[i], queryTF_IDF);
		    	System.out.println("queryTF_IDF " + queryTF_IDF);
		    	
				for (String col : cols) {
				    System.out.println("j " + j + " col " + col);
				    j++; 
				    
//				    queryIDF = Math.log((numDocs)*1.0/numDocsWithTerm);
//				    System.out.println("queryIDF " + queryIDF);
//				    System.out.println("queryTF.get(queryParts[i]) " + queryTF_map.get(queryParts[i]));
//			    	queryTF_IDF = queryTF_map.get(queryParts[i]) * queryIDF;
//			    	queryTF_IDF_map.put(queryParts[i], queryTF_IDF);
//			    	System.out.println("queryTF_IDF " + queryTF_IDF);
			    	
			    	String tf_idf = r.get(col);
			    	System.out.println("tf_idf " + tf_idf);
			    	double docTF_IDF = Double.parseDouble(tf_idf);
			    	System.out.println("docTF_IDF " + docTF_IDF);
			    	
			    	
				    if(!computedVals.containsKey(col)) {
				    	computedVals.put(col, String.valueOf(docTF_IDF * queryTF_IDF));
				    	System.out.println("!computedVals.contains(col) new val added " + String.valueOf(docTF_IDF * queryTF_IDF));

				    }
				    else  {
				    	String prevVal = computedVals.get(col);
				    	System.out.println("prevVal " + prevVal);
				    	double prevValDouble = Double.parseDouble(prevVal);
				    	System.out.println("prevValDouble " + prevValDouble);
				    	double newVal = docTF_IDF* queryTF_IDF + prevValDouble;
				    	System.out.println("newVal " + String.valueOf(newVal));
				    	computedVals.put(col, String.valueOf(newVal));
				    }
				    docWeight += Math.pow(docTF_IDF, 2);
				    System.out.println("docWeight " + String.valueOf(docWeight));
				}
				
				
		    	
		    	
			}
		}
		
		double queryWeight = 0.0;
		for(Map.Entry<String, Double> e: queryTF_IDF_map.entrySet()) {
			queryWeight += Math.pow(queryTF_IDF_map.get(e.getKey()), 2);
			System.out.println("queryWeight in for loop " + String.valueOf(queryWeight));
		}
		queryWeight = Math.sqrt(queryWeight);
		System.out.println("queryWeight " + String.valueOf(queryWeight));
		
    	//Doc weight
    	docWeight = Math.sqrt(docWeight);
    	System.out.println("docWeight after sqrt " + String.valueOf(docWeight));

    	for(Map.Entry<String, String> e:computedVals.entrySet()) {
    		String col = e.getKey();
    		System.out.println("col " + col);
    		double cosSim = Double.parseDouble(computedVals.get(col)) / (docWeight * queryWeight);
    		System.out.println("cosSim " + String.valueOf(cosSim));
    		
    		String hashedVal = Hasher.hash(col);
		    Row pageRank = kvs.getRow("pt-pageranks", hashedVal);
		    double newVal = 0.0;
		    if(kvs.existsRow("pt-pageranks", hashedVal)){
		    	String data = pageRank.get("rank");
		    	System.out.println("data " + data);
			    double dataDouble = Double.parseDouble(data);
			    System.out.println("dataDouble " + String.valueOf(dataDouble));
		    	 newVal= dataDouble + cosSim;
		    	 System.out.println("tfidfxx " + cosSim + " pagerank "+dataDouble);
		    }
		    else {
		    	 newVal= cosSim;
		    }
		    

    		//double newVal = cosSim;
	    	
	    	System.out.println("newVal " + String.valueOf(newVal));
	    	computedVals.put(col, String.valueOf(newVal));
    	}
		
		 computedVals.entrySet()
         .stream()
         .sorted(Map.Entry.<String, String>comparingByValue(Comparator.comparingDouble(Double::parseDouble).reversed()))
         .forEach(entry -> System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue()));
		 
		 
		 List<Map.Entry<String, String>> list = new ArrayList<>(computedVals.entrySet());
	        list.sort((entry1, entry2) -> {
	            Double value1 = Double.parseDouble(entry1.getValue());
	            Double value2 = Double.parseDouble(entry2.getValue());
	            return value2.compareTo(value1);
	        });
		 
        Map<String, String> sortedMap = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        
        
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{");
        jsonBuilder.append("\"data\":[");
        int size = sortedMap.size();
        int count = 0;
		
        for (Map.Entry<String, String> entry : sortedMap.entrySet()) {
			
        	//get title
        	String title = "";
        	try {
	        	String hashedVal = Hasher.hash(entry.getKey());
	        	Row r = kvs.getRow("pt-crawl", hashedVal);
	        	String pageContent = r.get("page");
	        	
	        	
//	        	int startIndex = pageContent.indexOf("<title>") + "<title>".length();
//	            int endIndex = pageContent.indexOf("</title>");
//	            title = pageContent.substring(startIndex, endIndex);
	        	
	        	String head = extractHead(pageContent);
	        	title = extractTitle(head);
	            System.out.println("title " + title);
        	}catch(Exception e)
        	{
        		System.out.println("exception caught");
        		title = "";
        	}
        			
        	if(title == null) {
        		title = "";
        	}
        	 String escapedTitle = escapeJson(title);

        	    jsonBuilder.append("{")
        	        .append("\"url\":\"").append(entry.getKey()).append("\",")
        	        .append("\"title\":\"").append(escapedTitle).append("\"")
        	        .append("}");

				//if (++count < size) jsonBuilder.append(", ");
        	    if (++count < 15) jsonBuilder.append(", ");
				if(count == 15) break;
        	}

        	jsonBuilder.append("]}");
        try {
			jsonOutput =  jsonBuilder.toString();
			
			System.out.println("json output " + jsonOutput);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	        
	        
//	    k.put("pt-cache", query, "1", jsonOutput);    
        
	      return jsonOutput;
	     

	}
	
	private String escapeJson(String input) {
	    return input.replace("\\", "\\\\")
	                .replace("\"", "\\\"")
	                .replace("\b", "\\b")
	                .replace("\f", "\\f")
	                .replace("\n", "\\n")
	                .replace("\r", "\\r")
	                .replace("\t", "\\t");
	}
	
	private static String extractHead(String html) {
        Pattern headPattern = Pattern.compile("<head[^>]*>(.*?)</head>", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
        Matcher matcher = headPattern.matcher(html);
        if (matcher.find()) {
            return matcher.group(1); // Return content within the head tag
        }
        return null;
    }

    private static String extractTitle(String headContent) {
        if (headContent != null) {
            Pattern titlePattern = Pattern.compile("<title[^>]*>(.*?)</title>", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
            Matcher matcher = titlePattern.matcher(headContent);
            if (matcher.find()) {
                return matcher.group(1); // Return content within the title tag
            }
        }
        return null;
    }

}
