package cl.uchile.dcc.caching.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.zip.GZIPInputStream;

import org.apache.poi.ss.usermodel.Workbook;

import com.monitorjbl.xlsx.StreamingReader;

public class FilterLog {
  public static void main(String[] args) throws Exception {
	final BufferedReader tsv = 
		        new BufferedReader (
		                new InputStreamReader(
		                        new GZIPInputStream(
		                                new FileInputStream(
		                                        new File("D:\\wikidata_logs\\2017-07-10_2017-08-06_all.tsv.gz")))));
	final PrintWriter w = new PrintWriter(new FileWriter("D:\\wikidata_logs\\FilteredLogs.tsv"));
	
	InputStream is = new GZIPInputStream(new FileInputStream(new File("D:\\wikidata_logs\\2017-07-10_2017-08-06_all.tsv.gz")));
	
	Workbook workbook = StreamingReader.builder()
	        .rowCacheSize(100)    // number of rows to keep in memory (defaults to 10)
	        .bufferSize(4096)     // buffer size to use when reading InputStream to file (defaults to 1024)
	        .open(is);			  // InputStream or File for XLSX file (required)
	
	System.out.println(workbook.getSheetAt(0).getSheetName());
	
	tsv.readLine();
	String text = tsv.readLine();
	int line = 1;
	/*while (text != null) {
	  System.out.println("Reading Line: " + line++);
	  String[] filter = text.split("\t");
	  String dateTime = filter[1];
	  String[] filterTwo = dateTime.split(" ");
	  String dateFilter = "2017-08-03";
	  if (filterTwo[0].equals(dateFilter)) w.println(text);
	  text = tsv.readLine();
	}*/
	w.close();
  }
}
