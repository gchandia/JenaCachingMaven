package cl.uchile.dcc.caching.queries;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.resultset.ResultSetException;
import org.apache.jena.tdb.TDBFactory;

import cl.uchile.dcc.caching.common_joins.Parser;

public class ExecuteAllQueries {
	static long queryNumber = 1;
	//private static String myModel = "D:\\tmp\\WikiDB";
	private static String myModel = "/home/gchandia/WikiDB";
	private static Dataset ds = TDBFactory.createDataset(myModel);
	
	public static void main(String[] args) throws Exception {
	  InputStream is = new FileInputStream(new File("/home/gchandia/wikidata_logs/FilteredLogs.tsv"));
	  //InputStream is = new FileInputStream(new File("D:\\wikidata_logs\\FilteredLogs.tsv"));
	  final Scanner sc = new Scanner(is);
	  // Read my TDB dataset
	  //String dbDir = "/home/gchandia/WikiDB";
	  // Write if I wanna write, but I'll be using read to query over it mostly
	  ds.begin(ReadWrite.READ);
	  
	  // Define model and Query
	  final Model model = ds.getDefaultModel();
	  
	  final PrintWriter w = new PrintWriter(new FileWriter("/home/gchandia/Thesis/NoCacheFinalV2.txt"));
	  //final PrintWriter w = new PrintWriter(new FileWriter("D:\\Thesis\\NoCacheFinal.txt"));
	  
	  for (int i = 1; i <= 250000; i++) {
		final Runnable stuffToDo = new Thread() {
		@Override 
		  public void run() { 
			try {
				System.out.println("READING QUERY " + queryNumber++);
				String line = sc.nextLine();
				Parser parser = new Parser();
				Query q = parser.parseDbPedia(line);
				
				// Create query
				ds.begin(ReadWrite.READ);
				QueryExecution exec = QueryExecutionFactory.create(q, model);
				long start = System.nanoTime();
				ResultSet results = exec.execSelect();
				int resultAmount = 0;
				
				while (results.hasNext()) {
				  results.next();
				  resultAmount++;
				}
				
				long stop = System.nanoTime();
				
				w.println("Info for query number " + (queryNumber - 1));
				w.println(line);
				w.println("Number of results: " + resultAmount);
				w.println("Time after reading all results: " + (stop - start));
				w.flush();
				
			} catch (IllegalArgumentException e) {}
			  catch (ResultSetException e) {}
			  catch (QueryParseException e) {}
			  catch (IOException e) {}
			  catch (Exception e) {}
			}
		};
		
		final ExecutorService executor = Executors.newSingleThreadExecutor();
		@SuppressWarnings("rawtypes")
		final Future future = executor.submit(stuffToDo);
		executor.shutdown(); // This does not cancel the already-scheduled task.
		
		try { 
		  future.get(15, TimeUnit.SECONDS); 
		} catch (InterruptedException ie) {}
		  catch (ExecutionException ee) {}
		  catch (TimeoutException te) {}
		}
		
		w.close();
		sc.close();
	}
}