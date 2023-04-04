package cl.uchile.dcc.caching.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Scanner;

import org.apache.jena.query.Query;

import cl.uchile.dcc.caching.common_joins.Parser;

public class ReadLogs {
  public static void main(String[] args) throws Exception {
	InputStream is = new FileInputStream(new File("D:\\wikidata_logs\\FilteredLogs.tsv"));
	final Scanner sc = new Scanner(is);
	for (int i = 1; i <= 10; i++) {
	  String qu = sc.nextLine();
	  Parser parser = new Parser();
	  Query q = parser.parseDbPedia(qu);
	  System.out.println(q);
	}
    sc.close();
  }
}
