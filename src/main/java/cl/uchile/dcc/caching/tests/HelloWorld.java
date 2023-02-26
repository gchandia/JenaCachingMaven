package cl.uchile.dcc.caching.tests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class HelloWorld {
	private static HashMap<String, Integer> ints = new HashMap<String, Integer>();
	private static LinkedHashMap<String, Integer> sortedInts = new LinkedHashMap<String, Integer>();
	private static LinkedHashMap<String, Integer> unsortedInts = new LinkedHashMap<String, Integer>();
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
		
		unsortedInts.put("Four", 4);
		unsortedInts.put("One", 1);
		unsortedInts.put("Three", 3);
		unsortedInts.put("Five", 5);
		unsortedInts.put("Two", 2);
		
		blocks.add("1");
		blocks.add("2");
		blocks.add("3");
		blocks.add("4");
		blocks.add("5");
		
		System.out.println(unsortedInts.get("Four"));
		System.out.println(unsortedInts.get("Six"));
		System.out.println(Long.valueOf(10).doubleValue());
		System.out.println(2 * 1.0);
		System.out.println(unsortedInts);
		List<Map.Entry<String, Integer>> entries = new ArrayList<Map.Entry<String, Integer>>(unsortedInts.entrySet());
		Collections.sort(entries, new Comparator<Map.Entry<String, Integer>>() {
			  public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b){
			    return a.getValue().compareTo(b.getValue());
			  }
			});
		System.out.println(entries);
	}
}
