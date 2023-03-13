package cl.uchile.dcc.caching.tests;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Scanner;

public class CountLines {
  public static void main(String[] args) throws Exception {
    InputStream is = new FileInputStream(new File("D:\\wikidata_logs\\FilteredLogs.tsv"));
	Scanner sc = new Scanner(is);
	int lines = 0;
	while (sc.hasNextLine()) {
	  lines++;
	  sc.nextLine();
	}
	System.out.println(lines);
	sc.close();
  }
}
