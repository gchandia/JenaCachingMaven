package cl.uchile.dcc.caching.experiments;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Scanner;

public class ReadFile {
  public static void main(String[] args) throws Exception {
	InputStream is = new FileInputStream(new File("D:\\Thesis\\RepeatedTriples8plus.txt"));
	final Scanner sc = new Scanner(is);
	
	while (sc.hasNextLine()) {
	  System.out.println(sc.nextLine());
	}
	
	sc.close();
  }
}
