package cl.uchile.dcc.caching.data_reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class SeparateFile {
	
	public static void separateFile(String file, String output, int copies) throws IOException {
		BufferedReader tsv = 
				new BufferedReader (
						new InputStreamReader(
								new FileInputStream(
												new File(file))));
		String init_line = tsv.readLine();
		for (int i = 1; i <= copies; i++) {
			PrintWriter w = new PrintWriter(new FileWriter(output + "_" + i + ".tsv"));
			w.println(init_line);
			for (int j = 1; j <= 1000; j++) {
				System.out.println("file " + i + " line " + j);
				String line = tsv.readLine();
				w.println(line);
			}
			w.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		separateFile("D:\\wikidata_logs\\wikidata_10K.tsv", "D:\\wikidata_logs\\wikidata_1K", 10);
	}
}