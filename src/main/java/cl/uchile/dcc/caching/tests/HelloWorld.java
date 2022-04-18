package cl.uchile.dcc.caching.tests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class HelloWorld {
	private static HashMap<String, Integer> ints = new HashMap<String, Integer>();
	private static LinkedHashMap<String, Integer> sortedInts = new LinkedHashMap<String, Integer>();
	private static ArrayList<String> blocks = new ArrayList<String>();
	
	public static void main(String[] args) {
		System.out.println("Hello world!");
		ints.put("One", 1);
		ints.put("Two", 2);
		ints.put("Three", 3);
		ints.put("Four", 4);
		ints.put("Five", 5);
		
		sortedInts.put("One", 1);
		sortedInts.put("Two", 2);
		sortedInts.put("Three", 3);
		sortedInts.put("Four", 4);
		sortedInts.put("Five", 5);
		sortedInts.put("Three", 6);
		
		blocks.add("1");
		blocks.add("2");
		blocks.add("3");
		blocks.add("4");
		blocks.add("5");
		
		System.out.println(blocks.indexOf("1"));
	}
}
