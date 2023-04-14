package cl.uchile.dcc.caching.experiments;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

public class FilterLog {
  public static void main(String[] args) throws FileNotFoundException, IOException {
	final PrintWriter w = new PrintWriter(new FileWriter("D:\\wikidata_logs\\FilteredLogs.tsv"));
	
	InputStream is = new GZIPInputStream(new FileInputStream(new File("D:\\wikidata_logs\\2017-07-10_2017-08-06_all.tsv.gz")));
	Scanner sc = new Scanner(is);
	
	String text = sc.nextLine();
	int line = 1;
	while (sc.hasNextLine()) {
	  System.out.println("Reading Line: " + line++);
	  try {
		String[] filter = text.split("\t");
		String dateTime = filter[1];
		String[] filterTwo = dateTime.split(" ");
		String dateFilter = "2017-07-12";
		if (filterTwo[0].equals(dateFilter)) w.println(text);
		text = sc.nextLine();
	  } catch (Exception e) {} 
	}
	sc.close();
	w.close();
  }
}
