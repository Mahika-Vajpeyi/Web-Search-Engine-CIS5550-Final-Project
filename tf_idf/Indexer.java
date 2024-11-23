package cis5550.jobs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.external.PorterStemmer;
import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class Indexer {
	
	private ConcurrentHashMap<String, ArrayList<FlamePair>> map = new ConcurrentHashMap<String, ArrayList<FlamePair>>();
	
	private static String stem(String input) {
		
		PorterStemmer stemmer = new PorterStemmer();
		stemmer.add(input.toCharArray(), input.length());
	    stemmer.stem();
	    return stemmer.toString();

    }
	
	public static void run(FlameContext context, String[] args) throws Exception
	{
		try {
			FlameRDD tableUrl = context.fromTable("pt-crawl", row -> {
				String u = row.get("url");
				String hashedU = Hasher.hash(u);
				String p = row.get("page");
				
				System.out.println("url " + u + " hashed u " + hashedU);
				//System.out.println("p " + p);
				
	            return u+","+p; 
	        });
			
			int totalUrlCount = tableUrl.count();
			
			FlamePairRDD flamePair = tableUrl.mapToPair(input -> {
				String[] stringPair = input.split(",", 2);
				FlamePair pair = new FlamePair(stringPair[0], stringPair[1]);
				return pair;
			});
			FlamePairRDD wordPair = flamePair.flatMapToPair(pair -> {
				String url = pair._1();
				String page = pair._2();
				
				page = page.replaceAll("(?s)<script[^>]*>(.*?)</script>", " ");
				page = page.replaceAll("(?s)<style[^>]*>(.*?)</style>", " ");
				
				String html = page.replaceAll("(?i)<nav[^>]*>(.*?)</nav>", " ");
			    html = html.replaceAll("(?i)<footer[^>]*>(.*?)</footer>", " ");
			    html = html.replaceAll("(?i)<head[^>]*>(.*?)</head>", " ");
//			    html = html.replaceAll("(?i)<form[^>]*>(.*?)</form>", " ");
			    html = html.replaceAll("(?i)<input[^>]*>", " ");
			    html = html.replaceAll("(?i)<textarea[^>]*>(.*?)</textarea>", " ");
			    html = html.replaceAll("(?i)<button[^>]*>(.*?)</button>", " ");
//			    html = html.replaceAll("(?i)<select[^>]*>(.*?)</select>", " ");
			    html = html.replaceAll("<!--(.*?)-->", " ");
				
				String text = page.replaceAll("<[^>]+>", " ");
				String formatted = text.replaceAll("\\t\\r\\n", " ");
				String onlyEnglish = formatted.replaceAll("[^\\x00-\\x7F]", " ");
				String alphabets = onlyEnglish.replaceAll("[^a-zA-Z ]", " ");
	            String words = alphabets.replaceAll("\\p{Punct}", " ").toLowerCase().replaceAll("[0-9]", " ");
	
	            
	            String[] wordList = words.split("\\s+");
	            

	            
	            Map<String, Integer> frequencyMap = new ConcurrentHashMap<>();
	            for (String word : wordList) {
	                if (!word.isEmpty()) {
	                    //word = stem(word);
	                    frequencyMap.put(word, frequencyMap.getOrDefault(word, 0) + 1);
	                }
	            }
	            
	            List<FlamePair> iterList = new ArrayList<>();
	            System.out.println("url " + url);
	            System.out.println("total words " + (wordList.length - 1));
	            
	            for (String word : frequencyMap.keySet()) {
	            	System.out.println("word: " + word);
	            	System.out.println("current value " + frequencyMap.get(word));
	            	System.out.println("current in double " + frequencyMap.get(word) * 1.0);
	            	double value = (frequencyMap.get(word)*1.0)/(wordList.length - 1);
	            	System.out.println("final val " + value);
	            	String strVal = url + "," + String.valueOf(value);
	                FlamePair newPair = new FlamePair(word, strVal); 
	                iterList.add(newPair);
	            }
	            
	            
	            
	            Iterable<FlamePair> iter = iterList;
	            
				return iter;
			});
			
			
			FlamePairRDD numDocs = wordPair.foldByKey("0", (accumulator, currVal) -> {
			int acc = Integer.parseInt(accumulator);
	        //int curr = Integer.parseInt(currVal);
	        
	        int newAccRank = 0;
	        if(currVal != null) {
	        	newAccRank= acc + 1;
	        }
	        
	
	        return String.valueOf(newAccRank);
	    });
			
			
			numDocs.saveAsTable("pt-numdocs");
			
			
			FlamePairRDD joinedTable = wordPair.join(numDocs);
			
			joinedTable.saveAsTable("pt-joined");
			
			FlamePairRDD computeVal = joinedTable.flatMapToPair(pair ->{
				
				String key = pair._1();
				String val = pair._2();
				
				System.out.println("key " + key + " val " + val);
				
				List<FlamePair> iterList = new ArrayList<>();
				
				
				String [] parts = val.split(",", 3);
				
				System.out.println("parts[2] " + parts[2]);
				
				double den = Double.parseDouble(parts[2]);
				System.out.println("total url count " + totalUrlCount + " den " + den);
				double logVal = Math.log(totalUrlCount / den);
				
				System.out.println("log val " + logVal);
				
				System.out.println("parts[1] " + parts[1]);
				
				double tf = Double.parseDouble(parts[1]);
				
				System.out.println("tf " + tf);
				
				String result = String.valueOf(tf * logVal);
				
				FlamePair newPair = new FlamePair(key, result);
	            
				iterList.add(newPair);
	            
				KVSClient k = context.getKVS();
	
				k.put("pt-computed", key, parts[0], result);
				
//				Row row = new Row(key);
//				Set<String> rows = row.columns();
//				System.out.println("*** row size *** " + rows.size());
				
	            return Collections.emptyList();
				
			});
			

			
			//System.out.println("agg " + aggregate);
			wordPair.saveAsTable("pt-index");
			//System.out.println("table saved");
	} catch(Exception e) {
		System.out.println("Exception occured " + e);
	}

	}
	
	
	

	

}
