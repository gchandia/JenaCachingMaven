package cl.uchile.dcc.caching.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class GetCustomResults {
	public static void main(String[] args) {
	    try {
	      final BufferedReader tsv = 
	          new BufferedReader (
	                  new FileReader(
	                      new File("D:\\Thesis\\AverageResults.txt")));
	      
	      StringBuffer sb1 = new StringBuffer();
	      sb1.append("[");
	      
	      StringBuffer sb2 = new StringBuffer();
	      sb2.append("[");
	      
	      StringBuffer sb3 = new StringBuffer();
	      sb3.append("[");
	      
	      String line = tsv.readLine();
	      
	      while(line != null) {
	        String[] subs = line.split(" ");
	        if (subs[0].equals("Number") && subs[1].equals("of") && subs[2].equals("results")) {
	          sb1.append(subs[6]);
	          sb1.append(",");
	        } else if (subs[0].equals("Reading") && subs[1].equals("one")) {
	          sb2.append(subs[3]);
	          sb2.append(",");
	        } else if (subs[0].equals("Reading") && subs[1].equals("all")) {
	          sb3.append(subs[3]);
	          sb3.append(",");
	        }
	        line = tsv.readLine();
	      }
	      
	      sb1.append("]");
	      sb2.append("]");
	      sb3.append("]");
	      tsv.close();
	      String sbs1 = sb1.toString();
	      String sbs2 = sb2.toString();
	      String sbs3 = sb3.toString();
	      System.out.println(sbs1);
	      System.out.println(sbs2);
	      System.out.println(sbs3);
	    } catch(Exception e) {}
	  }
}
