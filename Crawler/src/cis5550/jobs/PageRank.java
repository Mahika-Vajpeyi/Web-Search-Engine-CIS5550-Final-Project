package cis5550.jobs;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class PageRank {
	public static List<String> urlExtract(byte[] data) {
		ArrayList<String> urls = new ArrayList<String>();
		String htmlText = new String(data);
		Pattern pattern = Pattern.compile("<([^/][^>]*)>");
		Matcher matcher = pattern.matcher(htmlText);
		int urlCount = 0;
		while (matcher.find()) {
			try {
				String tag = matcher.group(1);
				String[] tagParts = tag.split("\\s+");
				if (tagParts.length >= 1) {
					String tagType = tagParts[0];
					if (tagType.toLowerCase().equals("a")) {
						for (String s: tagParts) {
							if(s.contains("href=")) {
								int idx = s.indexOf('=');
								String url = s.substring(idx + 1);
								if (url.length() > 1) {
									String finalUrl = url.substring(1, url.length() - 1);
									urls.add(finalUrl);
									// urlCount ++;
									// if (urlCount >= 70) {
									// 	return urls;
									// }
								}
							}
						}
					}
				}
			} catch (Exception e) {
				return urls;
			}
		}
		return urls;
	}
	
	public static List<String> urlNormalize(List<String> urls, String base) {
		ArrayList<String> normalizedUrls = new ArrayList<String>();
		String baseUrl = base;
		for (String url : urls) {
			try {
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
//					Either relative or absolute path without hostname
//					Cuts off part in base URL after last /
					String modifiedBaseUrl = baseUrl.substring(0, baseUrl.lastIndexOf('/'));
					if (rest.length() == 0 || rest.charAt(0) == '/') {
//						absolute path
						String[] baseUrlParts = URLParser.parseURL(baseUrl);
						modifiedBaseUrl = baseUrlParts[0] + "://" + baseUrlParts[1] + ":" + baseUrlParts[2];
						normalizedUrls.add(modifiedBaseUrl+rest);
						continue;
					} else {
						while (rest.length() >= 3 && rest.substring(0, 3).equals("../")) {
							if (modifiedBaseUrl.lastIndexOf('/') > -1) {
								modifiedBaseUrl = modifiedBaseUrl.substring(0, modifiedBaseUrl.lastIndexOf('/'));
							}
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
			} catch (Exception e) {
				continue;
			}
		}
		return normalizedUrls;
	}
	
	public static void run(FlameContext ctx, String[] strArr) {
		if (strArr.length < 1 || strArr[0] == null) {
			return;
		}
		final double t = Double.parseDouble(strArr[0]);
		
		double perc = 100;
		if (strArr.length >= 2 && strArr[1] != null) {
			perc = Double.parseDouble(strArr[1]);
		}

		try {
//			1. Loading data into PairRDD
			RowToString lambdaOne = r -> {
				if(r.get("page") != null && r.get("url") != null) {
					List<String> extractedUrls = urlExtract(r.getBytes("page"));
					List<String> normalizedUrls = urlNormalize(extractedUrls, r.get("url"));
					Set<String> deduplicateUrls = new ConcurrentSkipListSet<String>();
					for (String s : normalizedUrls) {
						deduplicateUrls.add(s);
					}
					String fullString = "1.0,1.0";
					for (String url : deduplicateUrls) {
						if (!r.get("url").equals(url)) {
							fullString += "," + Hasher.hash(url);
						}
					}
					return Hasher.hash(r.get("url")) + "," + fullString;
				}
				return null;
			};
			FlameRDD dataLoad = ctx.fromTable("pt-crawl", lambdaOne);
			
			StringToPair lambdaTwo = s -> {
				int commaIdx = s.indexOf(',');
				String urlHash = s.substring(0, commaIdx);
				String txt = s.substring(commaIdx+1);
				FlamePair pair = new FlamePair(urlHash, txt);
				return pair;
			};
//			Maps urlHash to rc,rp,L (L being list of links)
			FlamePairRDD stateTable = dataLoad.mapToPair(lambdaTwo);
//			stateTable.saveAsTable("stateTable");
			dataLoad.destroy();
//			stateTable.saveAsTable("oldStateTable");
			
			FlamePairRDD rankCalc;
			FlamePairRDD aggregatedRank;
			FlamePairRDD cleanedState;
			FlameRDD diffs;
			FlameRDD diffsExceed;
			
			int iter = 0;
			for (int iteration = 0; iteration < 2; iteration++) {
//			while (true) {
				System.out.println("Iter: " + iter++);
//				2. Finding transfer table
				PairToPairIterable lambdaThree = p -> {
					String urlHash = p._1();
					String pageRankInfo = p._2();
					String[] rankInfoArr = pageRankInfo.split(",");
					int n = rankInfoArr.length - 2;
					double curr = Double.parseDouble(rankInfoArr[0]);
					double prev = Double.parseDouble(rankInfoArr[1]);
					double v;
					if (n != 0) {
						v = 0.85 * curr / n;
					} else {
						v = 0;
					}
//					v = Double.min(0.85,Double.max(0.01,v));
					ArrayList<FlamePair> pairIterable = new ArrayList<FlamePair>();
					for (int i=2; i<rankInfoArr.length; i++) {
						FlamePair fp = new FlamePair(rankInfoArr[i], String.valueOf(v));
						pairIterable.add(fp);
					}
					pairIterable.add(new FlamePair(urlHash, String.valueOf(0.0)));
					return pairIterable;
				};
				rankCalc = stateTable.flatMapToPair(lambdaThree);
				System.out.println("Finished state table");
				
//				3. Aggregating transfers
				TwoStringsToString lambdaFour = (s1, s2) -> {
					double s1Double = Double.parseDouble(s1);
					double s2Double = Double.parseDouble(s2);
					return String.valueOf(s1Double + s2Double);
				};
				aggregatedRank = rankCalc.foldByKey("0.0", lambdaFour);
				rankCalc.destroy();
				System.out.println("Aggregated transfers");
//				aggregatedRank.saveAsTable("aggregated");
				
				
//				4. Joining tables, cleaning joined result
				FlamePairRDD joined = stateTable.join(aggregatedRank);
				aggregatedRank.destroy();
				
				PairToPairIterable lambdaFive = p -> {
					String urlHash = p._1();
					String joinedState = p._2();
					String[] joinedStateSplit = joinedState.split(",");
					String currRank = joinedStateSplit[0];
					String prevRank = joinedStateSplit[1];
					String newRank = joinedStateSplit[joinedStateSplit.length - 1];
//					System.out.println(currRank + " " + prevRank + " " + newRank);
					double newRankFull = Double.parseDouble(newRank) + 0.15;
//					System.out.println(newRankFull);
					String[] newInfo = new String[joinedStateSplit.length - 1];
					newInfo[0] = String.valueOf(newRankFull);
					newInfo[1] = currRank;
					for (int i = 2; i<newInfo.length; i++) {
						newInfo[i] = joinedStateSplit[i];
					}
					String finalString = "";
					for (int i = 0; i<newInfo.length; i++) {
						if (i == newInfo.length - 1) {
							finalString += newInfo[i];
						} else {
							finalString += (newInfo[i] + ",");
						}
					}
					ArrayList<FlamePair> pairIterable = new ArrayList<FlamePair>();
					pairIterable.add(new FlamePair(urlHash, finalString));
					return pairIterable;
				};
				cleanedState = joined.flatMapToPair(lambdaFive);
				joined.destroy();
//				cleanedState.saveAsTable("newStateTable");
				System.out.println("Joined tables");
				
				stateTable = cleanedState;
//				cleanedState.destroy();
				PairToStringIterable lambdaSix = p -> {
					String urlHash = p._1();
					String joinedState = p._2();
					String[] joinedStateSplit = joinedState.split(",");
					double currRankD = Double.parseDouble(joinedStateSplit[0]);
					double prevRankD = Double.parseDouble(joinedStateSplit[1]);
					double diff = Math.abs(currRankD - prevRankD);
					System.out.println(diff);
					ArrayList<String> stringIterable = new ArrayList<String>();
					stringIterable.add(String.valueOf(diff));
					return stringIterable;
				};
				diffs = stateTable.flatMap(lambdaSix);
				
				TwoStringsToString lambdaSeven = (s1, s2) -> {
					double s1D = Double.parseDouble(s1);
					double s2D = Double.parseDouble(s2);
					if (s2D > s1D) return String.valueOf(s2D);
					else return String.valueOf(s1D);
				};
				String max = diffs.fold("0.0", lambdaSeven);
				diffs.destroy();
				System.out.println("Max: " + max);
				if (Double.parseDouble(max) < t) {
					break;
				}
				
//				cleanedState.destroy();
//				aggregatedRank.destroy();
//				
////				Convergence and replacing
//				stateTable = cleanedState;
//				PairToStringIterable lambdaSix = p -> {
//					String urlHash = p._1();
//					String joinedState = p._2();
//					String[] joinedStateSplit = joinedState.split(",");
//					double currRankD = Double.parseDouble(joinedStateSplit[0]);
//					double prevRankD = Double.parseDouble(joinedStateSplit[1]);
//					double diff = Math.abs(currRankD - prevRankD);
//					ArrayList<String> stringIterable = new ArrayList<String>();
//					stringIterable.add(String.valueOf(diff));
//					return stringIterable;
//				};
//				diffs = stateTable.flatMap(lambdaSix);
//				
////				Goes through each diff and changes it to 1.0 if less than threshold, 0.0 else
//				StringToIterable lambdaSeven = s -> {
//					ArrayList<String> stringIterable = new ArrayList<String>();
//					double diff = Double.parseDouble(s);
//					if (diff <= t) {
//						stringIterable.add("1.0");
//					} else {
//						stringIterable.add("0.0");
//					}
//					return stringIterable;
//				};
//				diffsExceed = diffs.flatMap(lambdaSeven);
//				diffs.destroy();
//				
////				Sums over diffsExceed to get number of values of diffs less than threshold
//				TwoStringsToString lambdaEight = (s1, s2) -> {
//					double runSum = Double.parseDouble(s1);
//					double s2D = Double.parseDouble(s2);
////					System.out.println(String.valueOf(runSum + s2D));
//					return String.valueOf(runSum + s2D);
//				};
//				String folded = diffsExceed.fold("0.0", lambdaEight);
//				double lenDiffs = (double) diffs.count();
//				double summed = Double.parseDouble(folded);
//				if (100.0 * (summed/lenDiffs) >= perc) {
//					break;
//				}
			}
			
			System.out.println("About to add");
			PairToPairIterable lambdaNine = p -> {
				KVSClient kvs = ctx.getKVS();
				String urlHash = p._1();
				String joinedState = p._2();
				String[] joinedStateSplit = joinedState.split(",");
				String currRank = joinedStateSplit[0];
				kvs.put("pt-pageranks", urlHash, "rank", currRank);
				return new ArrayList<FlamePair>();
			};
			FlamePairRDD res = stateTable.flatMapToPair(lambdaNine);
			System.out.println("After add");

			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}