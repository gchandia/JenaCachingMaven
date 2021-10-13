package cl.uchile.dcc.caching.data_reader;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

public class Tsv_Writer {
	public static void writeBgps(File output, String[][] bgps) throws Exception {
		PrintWriter w = new PrintWriter(new FileWriter(output));
		
		for (String[] s : bgps) {
			w.println(s[0] + "\t" + s[1]);
		}
		w.close();
	}
}