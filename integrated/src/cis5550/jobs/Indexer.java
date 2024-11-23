package cis5550.jobs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDD.StringToPair;

public class Indexer {
	public static String pageClean(String pageData) {
		String lower = pageData.toLowerCase();
		String noHTML = lower.replaceAll("\\<.*?>"," ");
		String noPunc = noHTML.replaceAll("[.,:;!?'\"\\(\\)-]", " ");
		String noTabs = noPunc.replace("\t", " ");
		String noCR = noTabs.replaceAll("\\n", " ");
		String noLF = noCR.replaceAll("\\r", " ");
		return noLF;
	}
	
	public static void run(FlameContext ctx, String[] strArr) {
		try {
//			1. Loading data into PairRDD
			RowToString lambdaOne = r -> {
				if(r.get("page") != null && r.get("url") != null) {
					return r.get("url") + "," + r.get("page");
				}
				return null;
			};
			FlameRDD dataLoad = ctx.fromTable("pt-crawl", lambdaOne);
			
			StringToPair lambdaTwo = s -> {
				int commaIdx = s.indexOf(',');
				String url = s.substring(0, commaIdx);
				String txt = s.substring(commaIdx+1);
				FlamePair pair = new FlamePair(url, txt);
				return pair;
			};
//			Maps url to page data
			FlamePairRDD dataPairRDD = dataLoad.mapToPair(lambdaTwo);
			
//			2. Creating inverted index
			PairToPairIterable lambdaThree = p -> {
				String url = p._1();
				String pageData = p._2();
				String cleanedPage = pageClean(pageData);
				String[] words = cleanedPage.split(" ");
				Set<String> uniqueWords = new ConcurrentSkipListSet<String>();
				for (String w : words) {
					if (w != "") {
						uniqueWords.add(w);
					}
				}
				ArrayList<FlamePair> pairIterable = new ArrayList<FlamePair>();
				for (String word: uniqueWords) {
//					System.out.println(word);
					FlamePair fp = new FlamePair(word, url);
					pairIterable.add(fp);
				}
				return pairIterable;
			};
			FlamePairRDD wordUrlPairRDD = dataPairRDD.flatMapToPair(lambdaThree);
			
			TwoStringsToString lambdaFour = (s1, s2) -> {
				if (s1 == "") {
					return s2;
				} else {
					return s1 + "," + s2;
				}
			};
			FlamePairRDD invertedIndex = wordUrlPairRDD.foldByKey("", lambdaFour);
			invertedIndex.saveAsTable("pt-index");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
