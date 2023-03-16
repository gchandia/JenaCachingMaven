package cl.uchile.dcc.caching.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class GetTimesArray {
  
  public static void main(String[] args) {
    try {
      final BufferedReader tsv = 
          new BufferedReader (
                  new FileReader(
                      new File("D:\\Thesis\\buffer100K.txt")));
      
      StringBuffer sb = new StringBuffer();
      sb.append("[");
      String line = tsv.readLine();
      
      while(line != null) {
        String[] subs = line.split(" ");
        if (subs[0].equals("Time") && subs[1].equals("after") && subs[2].equals("reading")) {
          sb.append(subs[5]);
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
