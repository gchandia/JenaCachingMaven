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
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.tdb.TDBFactory;

import cl.uchile.dcc.caching.cache.SolutionCache;
import cl.uchile.dcc.caching.common_joins.Parser;
import cl.uchile.dcc.caching.transform.CacheTransformCopy;

public class ExperimentOneAH {
	private static String myModel = "data/DB10M";
	private static String myQueries = "data/five_queries_nc.tsv.gz";
	private static String myOutput = "data/FiveCacheQueriesV2.txt";
	
	private static boolean useThread = true;
	
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
		SolutionCache myCache;
		
		// Initialize a new Solution Cache
		myCache = new SolutionCache();
		
		final BufferedReader tsv = 
				new BufferedReader (
						new InputStreamReader(
								new GZIPInputStream(
										new FileInputStream(
												new File(myQueries)))));
		
		System.out.println(getNumberOfCompressedLines(myQueries));
		
		final PrintWriter w = new PrintWriter(new FileWriter(myOutput));
		
		Dataset ds = TDBFactory.createDataset(myModel);	
		
		// Only if first line is garbage
		//tsv.readLine();
		
		for (int i = 1; i <= 5; i++) {
			System.out.println("Reading query "+i);
			String line = tsv.readLine();
			
			System.out.println("Query "+i+"\t"+line);
	
			RunQueryThread stuffToDo = new RunQueryThread(ds,myCache,line,i);
			
			if(useThread) {
				ExecutorService executor = Executors.newSingleThreadExecutor();
				@SuppressWarnings("rawtypes")
				Future future = executor.submit(stuffToDo);
				executor.shutdown(); // This does not cancel the already-scheduled task.
				
				try {
					future.get(1, TimeUnit.MINUTES);
				} catch (InterruptedException ie) {}
				catch (ExecutionException ee) {}
				catch (TimeoutException te) {
					te.printStackTrace();
				}
			} else {
				stuffToDo.run();
			}
			
			w.println("Query "+i+"\t"+line);
			w.print(stuffToDo.getLog());
		}
		w.close();
		tsv.close();
		ds.close();
	}
	
	public static class RunQueryThread extends Thread {
		final Dataset ds;
		final SolutionCache cache;
		final String query;
		final int qid;
		
		final StringBuffer sblog = new StringBuffer();
		int cacheResultAmount;
		
		public RunQueryThread(Dataset d, SolutionCache c, String q, int id) {
			this.ds = d;
			this.cache = c;
			this.query = q;
			this.qid = id;
		}

		@Override
		public void run() {
			try {
				// Write if I wanna write, but I'll be using read to query over it mostly
				ds.begin(ReadWrite.READ);
				
				// Define model and Query
				Model model = ds.getDefaultModel();
				
				long startLine = System.nanoTime();
				
				Parser parser = new Parser();
				Query q = parser.parseDbPedia(query);
				
				long afterParse = System.nanoTime();
				sblog.append("Query " + qid + "\tTime to parse:\t" + (afterParse - startLine)+"\n");
				
				Op inputOp = Algebra.compile(q);
				
				Transform cacheTransform = new CacheTransformCopy(cache, startLine);
				Op cachedOp = Transformer.transform(cacheTransform, inputOp);
				
//				String solution = ((CacheTransformCopy) cacheTransform).getSolution();
				long beforeOptimize = System.nanoTime();
				sblog.append("Query " + qid + "\tTime before optimizing:\t" + (beforeOptimize - startLine)+"\n");
				
				Op opjoin = Algebra.optimize(cachedOp);
				
				long start = System.nanoTime();
				sblog.append("Query " + qid + "\tTime before reading results:\t" + (start - startLine)+"\n");
				
				QueryIterator cache_qit = Algebra.exec(opjoin, model);
				
				int cacheResultAmount = 0;
				
				while (cache_qit.hasNext()) {
					// can be interrupted externally after a result
					if(Thread.currentThread().isInterrupted())
						throw new InterruptedException();
					
					cache_qit.next();
					cacheResultAmount++;
				}
				
				this.cacheResultAmount = cacheResultAmount;
				
				long stop = System.nanoTime();
				sblog.append("Query " + qid + "\tTime after reading all results:\t" + (stop - startLine)+"\n");
				//TimeUnit.MINUTES.sleep(1);
				
				sblog.append("Query " + qid + "\tResults with cache:\t" + cacheResultAmount+"\n");
				
				ds.end();
			} catch (Exception e) {
				e.printStackTrace(System.out);
				ds.end();
			}
		}
		
		public int getCacheResultAmount() {
			return cacheResultAmount;
		}
		
		public String getLog() {
			return sblog.toString();
		}
	}
}
