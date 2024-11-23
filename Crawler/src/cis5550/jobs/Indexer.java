package cis5550.jobs;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
		String filePath = "src/cis5550/jobs/words.txt";
		//List<String> allWords = new ArrayList<String>();
		Map<String, Boolean> allWords = new ConcurrentHashMap<>();
		
		try {
            File file = new File(filePath);

            Scanner scanner = new Scanner(file);

            while (scanner.hasNextLine()) {
                String word = scanner.nextLine().trim();
                word = stem(word);
                
                
                if(word.length()>2)
                {
                	allWords.put(word, Boolean.TRUE);
                }
            }

            scanner.close();

            System.out.println("Words loaded: " + allWords.size());
        } catch (FileNotFoundException e) {
            System.out.println("File not found: " + filePath);
            e.printStackTrace();
        }
		
		
		try {
			FlameRDD tableUrl = context.fromTable("pt-crawl", row -> {
				String u = row.get("url");
				String hashedU = Hasher.hash(u);
				String p = row.get("page");
				
				//System.out.println("url " + u + " hashed u " + hashedU);
				//System.out.println("p " + p);
				p = u + "," + p;
				
	            return hashedU+","+p; 
	        });
			
			System.out.println("table url generation done");
			
			int totalUrlCount = tableUrl.count();
			
			FlamePairRDD flamePair = tableUrl.mapToPair(input -> {
				String[] stringPair = input.split(",", 2);
				FlamePair pair = new FlamePair(stringPair[0], stringPair[1]);
				return pair;
			});
			tableUrl.destroy();
			
			System.out.println("flamepair generation done");
			
			FlamePairRDD wordPair = flamePair.flatMapToPair(pair -> {
				String urlHashed = pair._1();
				String urlPage = pair._2();
				
				String[] urlPageParts = urlPage.split(",", 2);
				String url = urlPageParts[0];
				String page = urlPageParts[1];
				
//				page = page.replaceAll("(?s)<script[^>]*>(.*?)</script>", " ");
//				page = page.replaceAll("(?s)<style[^>]*>(.*?)</style>", " ");
//				
//				String html = page.replaceAll("(?i)<nav[^>]*>(.*?)</nav>", " ");
//			    html = html.replaceAll("(?i)<footer[^>]*>(.*?)</footer>", " ");
//			    html = html.replaceAll("(?i)<head[^>]*>(.*?)</head>", " ");
////			    html = html.replaceAll("(?i)<form[^>]*>(.*?)</form>", " ");
//			    html = html.replaceAll("(?i)<input[^>]*>", " ");
//			    html = html.replaceAll("(?i)<textarea[^>]*>(.*?)</textarea>", " ");
//			    html = html.replaceAll("(?i)<button[^>]*>(.*?)</button>", " ");
////			    html = html.replaceAll("(?i)<select[^>]*>(.*?)</select>", " ");
//			    html = html.replaceAll("<!--(.*?)-->", " ");
//				
//				String text = page.replaceAll("<[^>]+>", " ");
//				String formatted = text.replaceAll("\\t\\r\\n", " ");
//				String onlyEnglish = formatted.replaceAll("[^\\x00-\\x7F]", " ");
//				String alphabets = onlyEnglish.replaceAll("[^a-zA-Z ]", " ");
//	            String words = alphabets.replaceAll("\\p{Punct}", " ").toLowerCase().replaceAll("[0-9]", " ");
//	
	            
	            List<String> content = new ArrayList<>();
		        
		        Pattern headingPattern = Pattern.compile("<h[1-6][^>]*>(.*?)</h[1-6]>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
		        Matcher headingMatcher = headingPattern.matcher(page);
		        //extractContent(headingMatcher, content, false, words);
		        
		        try {
		        while (headingMatcher.find()) {
		            String text = headingMatcher.group(1);
		            text = text.replaceAll("<[^>]+>", " ");  // Remove all HTML tags
		            text = text.replaceAll("[^a-zA-Z ]", " ").toLowerCase();  // Remove all non-alphabetic characters, convert to lower case

		            String[] tokens = text.trim().split("\\s+");
		            for (String token : tokens) {
		            	token = stem(token);
		                if (!token.isEmpty() && allWords.containsKey(token)) {
		                	//System.out.println("word added to content " + token);
		                	content.add(token);

	                		// frequencyMap.put(token, frequencyMap.getOrDefault(token, 0.0) + 1);

		                }
		            }
		        }
		        }catch(Exception e)
		        {
		        	System.out.println("error in heading matcher");
		        }
		        
		        Pattern paragraphPattern = Pattern.compile("<p[^>]*>(.*?)</p>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
		        Matcher paragraphMatcher = paragraphPattern.matcher(page);
		        //extractContent(paragraphMatcher, content, true, words);
		        
		        try {
		        while (paragraphMatcher.find()) {
		            String text = paragraphMatcher.group(1);
		            text = text.replaceAll("<[^>]+>", " ");  // Remove all HTML tags
		            text = text.replaceAll("[^a-zA-Z ]", " ").toLowerCase();  // Remove all non-alphabetic characters, convert to lower case

		            String[] tokens = text.trim().split("\\s+");
		            for (String token : tokens) {
		            
		            	token = stem(token);
		                if (!token.isEmpty() && allWords.containsKey(token)) {
		                	//System.out.println("word added to content " + token);
		                	content.add(token);

	                		//frequencyMap.put(token, frequencyMap.getOrDefault(token, 0.0) + 1);

		                }
		            }
		        }
		        }catch(Exception e){
		        	System.out.println("error in paragraph matcher");
		        }
	            
	            
	            //String[] wordList = words.split("\\s+");
		        String[] wordList = content.toArray(new String[0]);
	            
		        
	            
	            Map<String, Integer> frequencyMap = new ConcurrentHashMap<>();
	            for (String word : wordList) {
	                if (!word.isEmpty()) {
	                    word = stem(word);
	                    if(allWords.containsKey(word))
	                    	frequencyMap.put(word, frequencyMap.getOrDefault(word, 0) + 1);
	                }
	            }
	            
	            List<FlamePair> iterList = new ArrayList<>();
	            //System.out.println("url " + url);
	            //System.out.println("total words " + (wordList.length - 1));
	            try {
	            for (String word : frequencyMap.keySet()) {
	            	//System.out.println("word: " + word);
	            	//System.out.println("current value " + frequencyMap.get(word));
	            	//System.out.println("current in double " + frequencyMap.get(word) * 1.0);
	            	double value = (frequencyMap.get(word)*1.0)/(wordList.length - 1);
	            	//System.out.println("final val " + value);
	            	String strVal = url + "," + String.valueOf(value);
	                FlamePair newPair = new FlamePair(word, strVal); 
	                iterList.add(newPair);
	            }
	            }catch(Exception e) {
	            	System.out.println("error in tf calculation");
	            }
	            
	            
	            
	            Iterable<FlamePair> iter = iterList;
	            
				return iter;
			});
			
			System.out.println("wordpair generation done");
		
			flamePair.destroy();		
			
			
			FlamePairRDD numDocs = wordPair.foldByKey("0", (accumulator, currVal) -> {
			int acc = Integer.parseInt(accumulator);
	        //int curr = Integer.parseInt(currVal);
	        
	        int newAccRank = 0;
	        if(currVal != null) {
	        	newAccRank= acc + 1;
	        }
	        
	
	        return String.valueOf(newAccRank);
	    });
			System.out.println("numDocs done");
			
			//numDocs.saveAsTable("pt-numdocs");
			
			
			FlamePairRDD joinedTable = wordPair.join(numDocs);
			
			//joinedTable.saveAsTable("pt-joined");
			wordPair.destroy();
			numDocs.destroy();
			
			joinedTable.saveAsTable("pt-joinedTable");
			
			FlamePairRDD computeVal = joinedTable.flatMapToPair(pair ->{
				
				String key = pair._1();
				String val = pair._2();
				
				//System.out.println("key " + key + " val " + val);
				
				List<FlamePair> iterList = new ArrayList<>();
				
				
				String [] parts = val.split(",", 3);
				
				//System.out.println("parts[2] " + parts[2]);
				
				double den = Double.parseDouble(parts[2]);
				//System.out.println("total url count " + totalUrlCount + " den " + den);
				double logVal = Math.log(totalUrlCount / den);
				
				//System.out.println("log val " + logVal);
				
				//System.out.println("parts[1] " + parts[1]);
				
				double tf = Double.parseDouble(parts[1]);
				
				//System.out.println("tf " + tf);
				
				String result = String.valueOf(tf * logVal);
				
				FlamePair newPair = new FlamePair(key, result);
	            
				iterList.add(newPair);
	            
				KVSClient k = context.getKVS();
				
				try {
	
				k.put("pt-computed", key, parts[0], result);
				}
				catch(Exception e)
				{
					System.out.println("table put failed");
					return Collections.emptyList();
				}
				
//				Row row = new Row(key);
//				Set<String> rows = row.columns();
//				System.out.println("*** row size *** " + rows.size());
				
	            return Collections.emptyList();
				
			});
			
			System.out.println("all done");
			//System.out.println("agg " + aggregate);
			//joinedTable.destroy();
			//wordPair.saveAsTable("pt-index");
			//System.out.println("table saved");
	} catch(Exception e) {
		System.out.println("Exception occured " + e);
	}
	}
}
