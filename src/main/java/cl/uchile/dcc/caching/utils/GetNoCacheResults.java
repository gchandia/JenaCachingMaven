package cl.uchile.dcc.caching.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class GetNoCacheResults {
  public static void main(String[] args) {
	try {
	  final BufferedReader tsv = 
	    new BufferedReader (
	      new FileReader(
	        new File("D:\\Thesis\\NoCacheFinal_2_NoSERVICE.txt")));
	  
	  StringBuffer sb = new StringBuffer();
	  sb.append("[");
	  String line = tsv.readLine();
	  int numberOfQueries = 0;
	  int numberOfZeroes = 0;
	  
	  while(line != null) {
	    String[] subs = line.split(" ");
	    if (subs[0].equals("Time") && subs[1].equals("after") && subs[2].equals("reading")) {
	      sb.append(subs[5]);
	      sb.append(",");
	    } else if (subs[0].equals("Info")) {
	      numberOfQueries++;
	    } else if (subs[0].equals("Number") && subs[3].equals("0")) {
	      numberOfZeroes++;
	    }
	    line = tsv.readLine();
	  }
	  
	  sb.append("]");
	  tsv.close();
	  String sbs = sb.toString();
	  System.out.println(sbs.length());
	  System.out.println(sbs);
	  System.out.println(numberOfQueries);
	  System.out.println(numberOfZeroes);
	} catch(Exception e) {}
  }
}
