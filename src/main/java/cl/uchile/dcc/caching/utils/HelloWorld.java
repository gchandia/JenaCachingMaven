package cl.uchile.dcc.caching.utils;

public class HelloWorld {
  public static void main(String[] args) {
	System.out.println("Hello World");
	String s = "/home/gchandia/wikidata_logs/FilteredLogs_1.tsv";
	int ss = s.indexOf("F");
	String sss = s.substring(s.indexOf("F"), s.length());
	System.out.println(sss);
  }
}
