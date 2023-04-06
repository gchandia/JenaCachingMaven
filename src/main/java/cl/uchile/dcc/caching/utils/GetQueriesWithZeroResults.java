package cl.uchile.dcc.caching.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

import cl.uchile.dcc.caching.common_joins.Parser;

public class GetQueriesWithZeroResults {
  public static void main(String[] args) {
	try {
	  final BufferedReader tsv = 
		new BufferedReader (
		  new FileReader(
			new File("D:\\Thesis\\NoCacheFinal_1_NoSERVICE.txt")));
	  final PrintWriter w = new PrintWriter(new FileWriter("D:\\Thesis\\QueriesWithZeroResults.txt"));
	  String line = tsv.readLine();
	  String query = "";
	  int queryNumber = 1;
	  Parser p = new Parser();
	  while (line != null) {
		if (line.contains("SELECT")) query = line;
		String[] subs = line.split(" ");
		if (subs[0].equals("Number")) {
		  if (subs[3].equals("0")) {
			w.println("Info for query number " + queryNumber++);
			w.println(p.parseDbPedia(query));
			w.println("");
		  }
		}
		line = tsv.readLine();
	  }
	  w.close();
	} catch (Exception e) {}
  }
}
