package cl.uchile.dcc.caching.common_joins;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.jena.query.Query;

public class Writer {
	private String output;
	
	public Writer(String folder) {
		this.output = folder;
	}
	
	public void writeQueries(HashSet<Query> map) throws IOException {
		System.out.println("Begin Writing");
		PrintWriter w = new PrintWriter(new FileWriter(this.output));
		int total = map.size();
		System.out.println(total);
		int i = 1;
		
		Iterator<Query> it = map.iterator();
		Query q = it.next();
		System.out.println(q);
		while (it.hasNext()) {
			w.println(it.next().toString());
			System.out.println("Writing line" + i++ + " of " + total);
		}
		w.close();
		System.out.println("Ended writing");
	}
	
	public void writeJoins(HashMap<String, Integer> map) throws IOException {
		System.out.println("Begin Writing");
		PrintWriter w = new PrintWriter(new FileWriter(this.output));
		int total = map.size();
		int i = 1;
		
		for (Entry<String, Integer> e : map.entrySet()) {
			w.println(e.getKey() + "\t" + e.getValue());
			System.out.println("Writing line" + i++ + " of " + total);
		}
		w.close();
		System.out.println("Ended writing");
	}
}