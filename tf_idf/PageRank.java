package cis5550.jobs;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class PageRank {
	
	public static List<String> urlNormalizer(List<String> urls, String base) throws MalformedURLException {
		
		ArrayList<String> normalizedUrls = new ArrayList<String>();
		try {
		
		String baseUrl = base;
		for (String url : urls) {
			String[] urlParts = URLParser.parseURL(url);
			String protocol = urlParts[0];
			String hostName = urlParts[1];
			String port = urlParts[2];
			String rest = urlParts[3];
			if (rest.indexOf('#') > -1) {
				rest = rest.substring(0, rest.indexOf('#'));
				if (protocol == null && hostName == null && port == null && rest.equals("")) {
					normalizedUrls.add(baseUrl);
					continue;
				}
			}
			if (rest.lastIndexOf('.') > -1) {
				String end = rest.substring(rest.lastIndexOf('.'));
				if (end.equals(".jpg") || end.equals(".jpeg") || end.equals(".gif") || end.equals(".png") || end.equals(".txt")) {
					continue;
				}
			}
			if (protocol == null && hostName == null && port == null) {
//				Either relative or absolute path without hostname
//				Cuts off part in base URL after last /
				String modifiedBaseUrl = baseUrl.substring(0, baseUrl.lastIndexOf('/'));
				if (rest.charAt(0) == '/') {
//					absolute path
					String[] baseUrlParts = URLParser.parseURL(baseUrl);
					modifiedBaseUrl = baseUrlParts[0] + "://" + baseUrlParts[1] + ":" + baseUrlParts[2];
					normalizedUrls.add(modifiedBaseUrl+rest);
					continue;
				} else {
					while (rest.substring(0, 3).equals("../")) {
						modifiedBaseUrl = modifiedBaseUrl.substring(0, modifiedBaseUrl.lastIndexOf('/'));
						rest = rest.substring(3);
					}
					normalizedUrls.add(modifiedBaseUrl+"/"+rest);
					continue;
				}
			}
			else if (port == null) {
				String modifiedBaseUrl = "";
				if (protocol.equals("http")) {
					modifiedBaseUrl = protocol + "://" + hostName + ":" + "80";
				} else if (protocol.equals("https")){
					modifiedBaseUrl = protocol + "://" + hostName + ":" + "443";
				} else {
					continue;
				}
				normalizedUrls.add(modifiedBaseUrl+rest);
				continue;
			}
			normalizedUrls.add(url);
		}
		}catch(Exception e){
			normalizedUrls.add("");
		}
		return normalizedUrls;

		
		 
	}
	
	public static List<String> urlExtract(byte[] data) {
		ArrayList<String> urls = new ArrayList<String>();
		String htmlText = new String(data);
		Pattern pattern = Pattern.compile("<([^/][^>]*)>");
		Matcher matcher = pattern.matcher(htmlText);
		while (matcher.find()) {
			String tag = matcher.group(1);
			String[] tagParts = tag.split("\\s+");
			String tagType = tagParts[0];
			if (tagType.toLowerCase().equals("a")) {
				for (String s: tagParts) {
					if(s.contains("href=")) {
						int idx = s.indexOf('=');
						String url = s.substring(idx + 1);
						String finalUrl = url.substring(1, url.length() - 1);
						urls.add(finalUrl);
					}
				}
			}
		}
		return urls;
	}
	
	public static List<String> extractUrls(String pageData) {
		//System.out.println("page data " + pageData);
        List<String> urls = new ArrayList<>();
        //TODO: confirm matcher
        Pattern pattern = Pattern.compile("<a\\s+(?:[^>]*?\\s+)?(?i)href=\"([^\"]*)\"", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(pageData);

        while (matcher.find()) {
            String url = matcher.group(1).trim();
            System.out.println("matched url " + url);
            urls.add(url);
        }

        return urls;
    }
	
	
	public static void run(FlameContext context, String[] stringArr) {
		
		String crawlTable = "pt-crawl";

		System.out.println("crawlTable " + crawlTable);
		try {
			
			
			    FlameRDD stateTable = context.fromTable("pt-crawl", row -> {
				String u = row.get("url");
				String p = row.get("page");
				System.out.println("url " + u + " page "+p);
				List<String> urls = new ArrayList<>();
				if(p!=null) {
					byte[] b = p.getBytes();
					//urls = urlExtract(b);
					urls = extractUrls(p);
				}
				Hasher hasher = new Hasher();
				
				List<String> normalizedUrls = new ArrayList<>();
				try {
					normalizedUrls = urlNormalizer(urls, u);
				} catch (MalformedURLException e) {
					e.printStackTrace();
				}
				
				LinkedHashSet<String> lset= new LinkedHashSet<>(); 
				LinkedHashSet<String> unhashed= new LinkedHashSet<>(); 
				for(int i = 0; i<normalizedUrls.size(); i++)
				{
					
					if(!normalizedUrls.get(i).equals("")) {
						String hashed = hasher.hash(normalizedUrls.get(i));
	            		lset.add(hashed);
	            		//lset.add(normalizedUrls.get(i));
	            	}
				}
				
				String l = String.join(",", lset);
				String test = String.join(",", unhashed);
	            
				String hashedu = hasher.hash(u);
	            return hashedu+",1.0,1.0,"+l; 
				
	        });
			    
			FlamePairRDD flamePair = stateTable.mapToPair(input -> {
				System.out.println("flame pair");
				String[] stringPair = input.split(",", 2);
				FlamePair pair = new FlamePair(stringPair[0], stringPair[1]);
				return pair;
			});
			
			while(true) {
				
				FlamePairRDD transferTable = flamePair.flatMapToPair(pair -> {
		            String u = pair._1(); 
		            String val = pair._2();
		            String[] parts = val.split(",", 3); 
		            double rc = Double.parseDouble(parts[0]); 
		            double rp = Double.parseDouble(parts[1]);
		            String links = parts[2]; 
		            String[] linkArray = links.split(","); 
		            int n = linkArray.length; 
		
		            List<FlamePair> outputPairs = new ArrayList<>();
		
		            for (String li : linkArray) {
		                if (!li.isEmpty()) { 
		                    double v = 0.85 * rc / n; 		                    
		                    FlamePair newPair = new FlamePair(li, String.valueOf(v));
		                    outputPairs.add(newPair);
		                }
		            }
		            FlamePair newPair0 = new FlamePair(u, String.valueOf(0.0));
		            
		        	outputPairs.add(newPair0);
		
		            Iterable<FlamePair> iter = outputPairs;
		            return iter;
		        });
				
				
				
				
				FlamePairRDD aggregatedRanks = transferTable.foldByKey("0.0", (accumulator, currVal) -> {
		            double accRank = Double.parseDouble(accumulator);
		            double currentRankValue = Double.parseDouble(currVal);
		            
		            double newAccRank = accRank + currentRankValue;
		            
		            return String.valueOf(newAccRank);
		        });
				
				FlamePairRDD joined = aggregatedRanks.join(flamePair);

				
				FlamePairRDD updatedStateTable = joined.flatMapToPair(joinedData -> {
		            String urlHash = joinedData._1(); 
		            String values = joinedData._2();
		            
		            System.out.println("urlHash " + urlHash);
		            System.out.println("values " + values);
		            
		            String[] stateVals = values.split(",", 4); 
		            stateVals[2] = stateVals[1];
		            stateVals[1] = String.valueOf(Double.parseDouble(stateVals[0]) + 0.15);
		            
		            String newStateVals = stateVals[1] +","+ stateVals[2] +","+ stateVals[3];
		            
		            System.out.println("new state vals " + newStateVals);
		            
		            List<FlamePair> outputPairs = new ArrayList<>();
		            FlamePair newPair = new FlamePair(urlHash, newStateVals);
		            outputPairs.add(newPair);
		            
		            Iterable<FlamePair> iter = outputPairs;
		            return iter;
		        });
				

				flamePair = updatedStateTable;

				
				FlameRDD rankChanges = flamePair.flatMap(pair -> {
	                String[] ranks = pair._2().split(",", 3);
	                double oldRank = Double.parseDouble(ranks[1]);
	                double newRank = Double.parseDouble(ranks[0]);
	                double rankChange = Math.abs(newRank - oldRank); 
	                
	                System.out.println("url for rank " + pair._1());
	                System.out.println("old rank " + oldRank + " new rank "+ newRank + " rank change " + rankChange);
	                
	                List<String> outputPairs = new ArrayList<>();
		            outputPairs.add(String.valueOf(rankChange));
		            
		            Iterable<String> iter = outputPairs;
		            return iter;
	
	            });
				
				
				String maxRankChange = rankChanges.fold("0.0", (acc, value) -> {
	            	double maxVal = Math.max(Double.parseDouble(acc), Double.parseDouble(value));
	            	System.out.println("acc val "+acc+" value val "+value+" maxval "+maxVal);
	            	return String.valueOf(maxVal);
	            });

		
					double maxDiff = Double.parseDouble(maxRankChange);

					//double convThreshold = Double.parseDouble(stringArr[0]);
					double convThreshold = 0.01;
					if(maxDiff < convThreshold) break;
			}
			
			
			
			flamePair.flatMapToPair(pair -> {
				
				KVSClient k = context.getKVS();
	            String url = pair._1();  
	            System.out.println("url " + url);
	            String[] values = pair._2().split(",", 3);
	            System.out.println("values " + pair._2());
	            double currentRank = Double.parseDouble(values[0]); 
	            //KVSClient kvs = context.getKVS();
	            System.out.println("curr rank " + currentRank);
	
	            k.put("pt-pageranks", url, "rank", String.valueOf(currentRank));
	
	            return Collections.emptyList();
	        });

	
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}


}
