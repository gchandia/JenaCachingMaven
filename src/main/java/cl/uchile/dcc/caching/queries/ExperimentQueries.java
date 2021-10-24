package cl.uchile.dcc.caching.queries;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.tdb.TDBFactory;

import cl.uchile.dcc.caching.common_joins.Parser;


public class ExperimentQueries {
	private static int j = 501;
	private static boolean running = false;
	
	public int getNumberOfLines(String input) throws Exception {
		int lines = 0;
		BufferedReader tsv = new BufferedReader(
				 new InputStreamReader(
						 new FileInputStream(new File(input))));
		while (tsv != null) {
			tsv.readLine();
			lines++;
		}
		
		return lines;
	}
	
	public static int getNumberOfCompressedLines(String input) throws Exception {
		int lines = 0;
		BufferedReader tsv = 
				new BufferedReader (
						new InputStreamReader(
								new GZIPInputStream(
										new FileInputStream(
												new File(input)))));
		
		String line = tsv.readLine();
		while (line != null) {
			line = tsv.readLine();
			lines++;
		}
		
		return lines;
	}
	
	public static void main(String[] args) throws Exception {
		// Read my TDB dataset
		String dbDir = "D:\\tmp\\WikiDB";
		Dataset ds = TDBFactory.createDataset(dbDir);
		// Write if I wanna write, but I'll be using read to query over it mostly
		ds.begin(ReadWrite.READ);
		// Define model and Query
		Model model = ds.getDefaultModel();
		BufferedReader tsv = 
				new BufferedReader (
						new InputStreamReader(
								new GZIPInputStream(
										new FileInputStream(
												new File("D:\\wikidata_logs\\2017-07-10_2017-08-06_organic.tsv.gz")))));
		System.out.println(getNumberOfCompressedLines("D:\\wikidata_logs\\2017-07-10_2017-08-06_organic.tsv.gz"));
		PrintWriter w = new PrintWriter(new FileWriter("D:\\tmp\\NoCacheQueries1000.txt"));
		
		//I want to do from lines 501-1500
		
		for (int i = 1; i <= 500; i++) {
			tsv.readLine();
		}
		
		for (int i = 1; i <= 1000; i++) {
			final Runnable stuffToDo = new Thread() {
				@Override
				public void run() {
					try {
						while (running) {
							System.out.println("waiting...");
						}
						
						running = true;
						ds.begin(ReadWrite.READ);
						
						System.out.println("Reading query " + j++);
						String line = tsv.readLine();
						
						long startLine = System.nanoTime();
						 
						Parser parser = new Parser();
						Query q = parser.parseDbPedia(line);
						
						long afterParse = System.nanoTime();
						String ap = "Time to parse: " + (afterParse - startLine);
						
						Op alg = Algebra.compile(q);
						
						long beforeOptimize = System.nanoTime();
						String bo = "Time before optimizing: " + (beforeOptimize - startLine);
						
						alg = Algebra.optimize(alg);
						
						long start = System.nanoTime();
						String br = "Time before reading results: " + (start - startLine);
						QueryIterator qit = Algebra.exec(alg, model);
						
						int resultAmount = 0;
						
						while (qit.hasNext()) {
							qit.next();
							resultAmount++;
						}
						
						long stop = System.nanoTime();
						String ar = "Time after reading all results: " + (stop - startLine);
						
						if (resultAmount != 0) {
							System.out.println("FOUND ONE");
							w.println("Info for query number " + (j-1));
						    w.println(q);
							w.println(ap);
							w.println(bo);
							w.println(br);
							w.println(ar);
							w.println("Query " + (j-1) +  " Results without cache: " + resultAmount);
							w.println("");
						}
						
						running = false;
						
					} catch (Exception e) {running = false;}
				}
			};
			
			final ExecutorService executor = Executors.newSingleThreadExecutor();
			@SuppressWarnings("rawtypes")
			final Future future = executor.submit(stuffToDo);
			executor.shutdown(); // This does not cancel the already-scheduled task.
			
			try { 
				  future.get(1, TimeUnit.MINUTES);
			} catch (InterruptedException ie) {running = false;}
			catch (ExecutionException ee) {running = false;}
			catch (TimeoutException te) {running = false;}
		}
		w.close();
	}

}
