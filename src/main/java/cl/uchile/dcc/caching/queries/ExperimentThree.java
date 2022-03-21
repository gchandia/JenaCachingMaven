package cl.uchile.dcc.caching.queries;

import java.util.ArrayList;
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
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.tdb.TDBFactory;

import cl.uchile.dcc.caching.bgps.ExtractBgps;
import cl.uchile.dcc.caching.cache.Cache;
import cl.uchile.dcc.caching.cache.LRUCache;
import cl.uchile.dcc.caching.common_joins.Parser;
import cl.uchile.dcc.caching.transform.CacheTransformCopy;
import cl.uchile.dcc.qcan.main.SingleQuery;

public class ExperimentThree {
	private static Cache myCache;
	private static String myModel = "D:\\tmp\\WikiDB";
	private static Dataset ds = TDBFactory.createDataset(myModel);
	
	public static void main(String[] args) throws Exception {

		// Write if I wanna write, but I'll be using read to query over it mostly
		ds.begin(ReadWrite.READ);
		
		// Define model and Query
		final Model model = ds.getDefaultModel();
		
		// Initialize a new Solution Cache
		myCache = new LRUCache(100, 1000000);
		
		String s50 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q3294251>\n"
				+ "  }";
		
		Query q50 = QueryFactory.create(s50);
		myCache.cacheConstants(q50);
		SingleQuery sq50 = new SingleQuery(q50.toString(), true, true, false, true);
		q50 = QueryFactory.create(sq50.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q50Bgps = ExtractBgps.getBgps(Algebra.compile(q50));
		//System.out.println(q10Bgps);
		QueryExecution q50Exec = QueryExecutionFactory.create(q50, model);
		ResultSet q50Results = q50Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q50Bgps.get(0), q50Results);
		
		
		final String q1 = "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q3294251> .\n"
				//+ "    ?x  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q3294251>\n"
				+ "  }";
		
		ds.end();
		
		for (int i = 1; i <= 1; i++) {
			final Runnable stuffToDo = new Thread() {
				@Override
				public void run() {
					try {
						ds.begin(ReadWrite.READ);
						
						//String line = tsv.readLine();
						String line = q1;
						
						Parser parser = new Parser();
						Query q = parser.parseDbPedia(line);
						
						Op inputOp = Algebra.compile(q);
						
						Transform cacheTransform = new CacheTransformCopy(myCache, 0);
						Op cachedOp = Transformer.transform(cacheTransform, inputOp);
						
						Op opjoin = Algebra.optimize(cachedOp);
						
						Query qFinal = OpAsQuery.asQuery(opjoin);
						
						QueryExecution qFinalExec = QueryExecutionFactory.create(qFinal, model);
						ResultSet rs = qFinalExec.execSelect();
						
						int cacheResultAmount = 0;
						
						while (rs.hasNext()) {
							rs.next();
							cacheResultAmount++;
						}
						
						System.out.println(cacheResultAmount);
						
					} catch (Exception e) {e.printStackTrace(System.out);}
				}
			};
			
			final ExecutorService executor = Executors.newSingleThreadExecutor();
			@SuppressWarnings("rawtypes")
			final Future future = executor.submit(stuffToDo);
			executor.shutdown(); // This does not cancel the already-scheduled task.
			
			try {
				future.get(1, TimeUnit.MINUTES);
			} catch (InterruptedException ie) {}
			catch (ExecutionException ee) {}
			catch (TimeoutException te) {}
		}
	}
}
