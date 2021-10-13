package cl.uchile.dcc.caching.common_joins;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;

/**
 * Special class that allows to join joins from different source files
 * @author gch1204
 *
 */
public class Joiner {
	private int nQueries;
	private int counter = 0;
	
	public void increaseLines() {
		this.nQueries++;
	}
	
	public void countLines(File folder) throws Exception {
		
		for (File entry : folder.listFiles()) {
			BufferedReader tsv = new BufferedReader(new InputStreamReader(new FileInputStream(entry)));
			while (true) {
				String line = tsv.readLine();
				if (line == null) break;
				increaseLines();
			}
			tsv.close();
		}
	}
	
	public int getLines() {
		return this.nQueries;
	}
	
	public HashMap<String, Integer> joinFolder(File folder) throws Exception {
		HashMap<String, Integer> map = new HashMap<String, Integer>(this.nQueries);
		for (File entry : folder.listFiles()) {
			if (!entry.isDirectory()) map.putAll(mapJoins(entry));
		}
		return map;
	}
	
	public HashMap<String, Integer> mapJoins(File entry) throws Exception {
		HashMap<String, Integer> map = new HashMap<String, Integer>(this.nQueries);
		BufferedReader tsv = new BufferedReader(new InputStreamReader(new FileInputStream(entry)));
		
		while (true) {
			try {
				String line = tsv.readLine();
				if (line == null) break;
				String join = line.substring(0, line.lastIndexOf("\t"));
				int total = Integer.parseInt(StringUtils.substringAfterLast(line, "\t"));
				if (map.get(join) == null) map.put(join, total);
				else map.replace(join, map.get(join) + total);
			} catch (NumberFormatException e) {}  // Exceptional case
			catch (StringIndexOutOfBoundsException e) {}
			System.out.println("Reading line " + this.counter++ + " of " + this.nQueries);
		}
		
		tsv.close();
		return map;
	}
}