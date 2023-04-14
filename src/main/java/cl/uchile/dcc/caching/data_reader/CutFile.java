package cl.uchile.dcc.caching.data_reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.zip.GZIPInputStream;

public class CutFile {
	
	static public void cut(String file, String output, int cutPoint) throws IOException {
		BufferedReader tsv = 
				new BufferedReader (
						new InputStreamReader(
								new GZIPInputStream(
										new FileInputStream(
												new File(file)))));
		PrintWriter w = new PrintWriter(new FileWriter(output));
		for (int i = 1; i <= cutPoint; i++) {
			String line = tsv.readLine();
			w.println(line);
		}
		w.close();
	}
	
	static public void main(String[] args) throws Exception {
		cut("D:\\wikidata_logs\\2017-07-10_2017-08-06_organic.tsv.gz", "D:\\wikidata_logs\\wikidata_10K.tsv", 10001);
	}
}