package cl.uchile.dcc.caching.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

public class CreateFilteredLogs {
  public static void main(String[] args) {
	try {
	  final BufferedReader tsv = 
	    new BufferedReader (
		  new FileReader(
			new File("D:\\wikidata_logs\\FilteredLogs.tsv")));
	  final PrintWriter fl1 = new PrintWriter(new FileWriter("D:\\wikidata_logs\\FilteredLogs_1.tsv"));
	  final PrintWriter fl2 = new PrintWriter(new FileWriter("D:\\wikidata_logs\\FilteredLogs_2.tsv"));
	  final PrintWriter fl3 = new PrintWriter(new FileWriter("D:\\wikidata_logs\\FilteredLogs_3.tsv"));
	  final PrintWriter fl4 = new PrintWriter(new FileWriter("D:\\wikidata_logs\\FilteredLogs_4.tsv"));
	  final PrintWriter fl5 = new PrintWriter(new FileWriter("D:\\wikidata_logs\\FilteredLogs_5.tsv"));
	  
	  int counter = 1;
	  
	  while (counter <= 50000) {fl1.println(tsv.readLine()); counter++;}
	  while (counter <= 100000) {fl2.println(tsv.readLine()); counter++;}
	  while (counter <= 150000) {fl3.println(tsv.readLine()); counter++;}
	  while (counter <= 200000) {fl4.println(tsv.readLine()); counter++;}
	  while (counter <= 250000) {fl5.println(tsv.readLine()); counter++;}
	  
	  fl1.close();
	  fl2.close();
	  fl3.close();
	  fl4.close();
	  fl5.close();
	} catch (Exception e) {}  
  }
}
