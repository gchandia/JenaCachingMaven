package cl.uchile.dcc.caching.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class GetCacheAttempts {
	public static void main(String[] args) {
	  try {
	      final BufferedReader tsv = 
	          new BufferedReader (
	                  new FileReader(
	                      new File("D:\\Thesis\\100KQueriesBuffer100K.txt")));
	  
	  StringBuffer sb = new StringBuffer();
	  sb.append("[");
	  String line = tsv.readLine();
	  
	  while(line != null) {
	    String[] subs = line.split(" ");
	    if (subs[0].equals("Number") && subs[1].equals("of") && subs[2].equals("bgps") && subs[3].equals("attempted")) {
	      sb.append(subs[6]);
	      sb.append(",");
	    }
	    line = tsv.readLine();
	  }
	  
	  sb.append("]");
	  tsv.close();
	  String sbs = sb.toString();
	  System.out.println(sbs.length());
	  System.out.println(sbs);
	} catch(Exception e) {}
  }
}
