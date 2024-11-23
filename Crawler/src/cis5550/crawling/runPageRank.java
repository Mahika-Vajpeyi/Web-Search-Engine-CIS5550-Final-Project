package cis5550.crawling;


public class runPageRank {
	
	public static void main(String args[]) throws Exception {
		System.out.println("Running the PageRank");
		String arg[] = new String[] { "0.1" };
		String output = FlameSubmit.submit("localhost:9000", "pagerank.jar", "cis5550.jobs.PageRank", arg);
		if (output != null) {
			System.out.println("Done");
		}
  }
}
