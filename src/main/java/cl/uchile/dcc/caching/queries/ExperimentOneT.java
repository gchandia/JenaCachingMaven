package cl.uchile.dcc.caching.queries;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;

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
import cl.uchile.dcc.caching.cache.SolutionCache;
import cl.uchile.dcc.caching.common_joins.Parser;
import cl.uchile.dcc.caching.transform.CacheTransformCopy;
import cl.uchile.dcc.qcan.main.SingleQuery;

public class ExperimentOneT {
	
	private static SolutionCache myCache;
	private static String myModel = "D:\\tmp\\WikiDB";
	private static Dataset ds = TDBFactory.createDataset(myModel);
	private static int j = 1;
	
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

		// Write if I wanna write, but I'll be using read to query over it mostly
		ds.begin(ReadWrite.READ);
		
		// Define model and Query
		final Model model = ds.getDefaultModel();
		
		// Initialize a new Solution Cache
		myCache = new SolutionCache();
		
		
		String s11 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P2949> ?v\n"
				+ "  }";
		
		Query q11 = QueryFactory.create(s11);
		myCache.cacheConstants(q11);
		SingleQuery sq11 = new SingleQuery(q11.toString(), true, true, false, true);
		q11 = QueryFactory.create(sq11.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q11Bgps = ExtractBgps.getBgps(Algebra.compile(q11));
		//System.out.println(q10Bgps);
		QueryExecution q11Exec = QueryExecutionFactory.create(q11, model);
		ResultSet q11Results = q11Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q11Bgps.get(0), q11Results);
		
		String s12 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P2678> ?v .\n"
				+ "  }";
		
		Query q12 = QueryFactory.create(s12);
		myCache.cacheConstants(q12);
		SingleQuery sq12 = new SingleQuery(q12.toString(), true, true, false, true);
		q12 = QueryFactory.create(sq12.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q12Bgps = ExtractBgps.getBgps(Algebra.compile(q12));
		//System.out.println(q10Bgps);
		QueryExecution q12Exec = QueryExecutionFactory.create(q12, model);
		ResultSet q12Results = q12Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q12Bgps.get(0), q12Results);
		
		
		String s13 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "{ ?x  <http://www.wikidata.org/prop/direct/P487> ?v .\n"
				+ "}";
		
		Query q13 = QueryFactory.create(s13);
		myCache.cacheConstants(q13);
		SingleQuery sq13 = new SingleQuery(q13.toString(), true, true, false, true);
		q13 = QueryFactory.create(sq13.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q13Bgps = ExtractBgps.getBgps(Algebra.compile(q13));
		//System.out.println(q10Bgps);
		QueryExecution q13Exec = QueryExecutionFactory.create(q13, model);
		ResultSet q13Results = q13Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q13Bgps.get(0), q13Results);
		
		
		String s14 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "{ ?x <http://www.wikidata.org/prop/direct/P1416> ?v .\n"
				+ "  ?v <http://www.wikidata.org/prop/direct/P50> ?y\n"
				+ "}";
		
		Query q14 = QueryFactory.create(s14);
		myCache.cacheConstants(q14);
		SingleQuery sq14 = new SingleQuery(q14.toString(), true, true, false, true);
		q14 = QueryFactory.create(sq14.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q14Bgps = ExtractBgps.getBgps(Algebra.compile(q14));
		//System.out.println(q10Bgps);
		QueryExecution q14Exec = QueryExecutionFactory.create(q14, model);
		ResultSet q14Results = q14Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q14Bgps.get(0), q14Results);
		
		String s15 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n" 
				+ "SELECT  ?var1\r\n"
				+ "WHERE\r\n"
				+ "  { ?var1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q11424> ;\r\n"
				+ "           <http://www.wikidata.org/prop/direct/P495>  <http://www.wikidata.org/entity/Q15180>\r\n"
				+ "    MINUS\r\n"
				+ "      { ?var1  <http://www.wikidata.org/prop/direct/P2678>  ?var2 }\r\n"
				+ "  }";
		
		Query q15 = QueryFactory.create(s15);
		myCache.cacheConstants(q15);
		SingleQuery sq15 = new SingleQuery(q15.toString(), true, true, false, true);
		q15 = QueryFactory.create(sq15.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q15Bgps = ExtractBgps.getBgps(Algebra.compile(q15));
		//System.out.println(q10Bgps);
		QueryExecution q15Exec = QueryExecutionFactory.create(q15, model);
		ResultSet q15Results = q15Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q15Bgps.get(0), q15Results);
		
		String s16 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P1258> ?v\n"
				+ "  }";
		
		Query q16 = QueryFactory.create(s16);
		myCache.cacheConstants(q16);
		SingleQuery sq16 = new SingleQuery(q16.toString(), true, true, false, true);
		q16 = QueryFactory.create(sq16.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q16Bgps = ExtractBgps.getBgps(Algebra.compile(q16));
		//System.out.println(q10Bgps);
		QueryExecution q16Exec = QueryExecutionFactory.create(q16, model);
		ResultSet q16Results = q16Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q16Bgps.get(0), q16Results);
		
		String s17 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P50> <http://www.wikidata.org/entity/Q20895241>\n"
				+ "  }";
		
		Query q17 = QueryFactory.create(s17);
		myCache.cacheConstants(q17);
		SingleQuery sq17 = new SingleQuery(q17.toString(), true, true, false, true);
		q17 = QueryFactory.create(sq17.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q17Bgps = ExtractBgps.getBgps(Algebra.compile(q17));
		//System.out.println(q10Bgps);
		QueryExecution q17Exec = QueryExecutionFactory.create(q17, model);
		ResultSet q17Results = q17Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q17Bgps.get(0), q17Results);
		
		String s18 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q1620797>\n"
				+ "  }";
		
		Query q18 = QueryFactory.create(s18);
		myCache.cacheConstants(q18);
		SingleQuery sq18 = new SingleQuery(q18.toString(), true, true, false, true);
		q18 = QueryFactory.create(sq18.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q18Bgps = ExtractBgps.getBgps(Algebra.compile(q18));
		//System.out.println(q10Bgps);
		QueryExecution q18Exec = QueryExecutionFactory.create(q18, model);
		ResultSet q18Results = q18Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(q18Bgps.get(0));
		//System.out.println(ResultSetFormatter.asText(q18Results));
		myCache.cache(q18Bgps.get(0), q18Results);
		
		String s19 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P106>  <http://www.wikidata.org/entity/Q3665646>\n"
				+ "  }";
		
		Query q19 = QueryFactory.create(s19);
		myCache.cacheConstants(q19);
		SingleQuery sq19 = new SingleQuery(q19.toString(), true, true, false, true);
		q19 = QueryFactory.create(sq19.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q19Bgps = ExtractBgps.getBgps(Algebra.compile(q19));
		//System.out.println(q10Bgps);
		QueryExecution q19Exec = QueryExecutionFactory.create(q19, model);
		ResultSet q19Results = q19Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q19Bgps.get(0), q19Results);
		
		String s20 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P105>  <http://www.wikidata.org/entity/Q38348>\n"
				+ "  }";
		
		Query q20 = QueryFactory.create(s20);
		myCache.cacheConstants(q20);
		SingleQuery sq20 = new SingleQuery(q20.toString(), true, true, false, true);
		q20 = QueryFactory.create(sq20.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q20Bgps = ExtractBgps.getBgps(Algebra.compile(q20));
		//System.out.println(q10Bgps);
		QueryExecution q20Exec = QueryExecutionFactory.create(q20, model);
		ResultSet q20Results = q20Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q20Bgps.get(0), q20Results);

		String s21 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P580>  ?v\n"
				+ "  }";
		
		Query q21 = QueryFactory.create(s21);
		myCache.cacheConstants(q21);
		SingleQuery sq21 = new SingleQuery(q21.toString(), true, true, false, true);
		q21 = QueryFactory.create(sq21.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q21Bgps = ExtractBgps.getBgps(Algebra.compile(q21));
		//System.out.println(q10Bgps);
		QueryExecution q21Exec = QueryExecutionFactory.create(q21, model);
		ResultSet q21Results = q21Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q21Bgps.get(0), q21Results);
		
		String s22 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P27>  <http://www.wikidata.org/entity/Q38>\n"
				+ "  }";
		
		Query q22 = QueryFactory.create(s22);
		myCache.cacheConstants(q22);
		SingleQuery sq22 = new SingleQuery(q22.toString(), true, true, false, true);
		q22 = QueryFactory.create(sq22.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q22Bgps = ExtractBgps.getBgps(Algebra.compile(q22));
		//System.out.println(q10Bgps);
		QueryExecution q22Exec = QueryExecutionFactory.create(q22, model);
		ResultSet q22Results = q22Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q22Bgps.get(0), q22Results);
		
		
		String s23 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q6256>\n"
				+ "  }";
		
		Query q23 = QueryFactory.create(s23);
		myCache.cacheConstants(q23);
		SingleQuery sq23 = new SingleQuery(q23.toString(), true, true, false, true);
		q23 = QueryFactory.create(sq23.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q23Bgps = ExtractBgps.getBgps(Algebra.compile(q23));
		//System.out.println(q10Bgps);
		QueryExecution q23Exec = QueryExecutionFactory.create(q23, model);
		ResultSet q23Results = q23Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q23Bgps.get(0), q23Results);
		
		
		String s24 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q42138>\n"
				+ "  }";
		
		Query q24 = QueryFactory.create(s24);
		myCache.cacheConstants(q24);
		SingleQuery sq24 = new SingleQuery(q24.toString(), true, true, false, true);
		q24 = QueryFactory.create(sq24.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q24Bgps = ExtractBgps.getBgps(Algebra.compile(q24));
		//System.out.println(q10Bgps);
		QueryExecution q24Exec = QueryExecutionFactory.create(q24, model);
		ResultSet q24Results = q24Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q24Bgps.get(0), q24Results);
		
		
		String s25 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P40>  <http://www.wikidata.org/entity/Q76>\n"
				+ "  }";
		
		Query q25 = QueryFactory.create(s25);
		myCache.cacheConstants(q25);
		SingleQuery sq25 = new SingleQuery(q25.toString(), true, true, false, true);
		q25 = QueryFactory.create(sq25.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q25Bgps = ExtractBgps.getBgps(Algebra.compile(q25));
		//System.out.println(q10Bgps);
		QueryExecution q25Exec = QueryExecutionFactory.create(q25, model);
		ResultSet q25Results = q25Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q25Bgps.get(0), q25Results);
		
		
		String s26 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q1248784>.\n"
				+ "?x <http://www.wikidata.org/prop/direct/P625>  ?var2\n"
				+ "  }";
		
		Query q26 = QueryFactory.create(s26);
		myCache.cacheConstants(q26);
		SingleQuery sq26 = new SingleQuery(q26.toString(), true, true, false, true);
		q26 = QueryFactory.create(sq26.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q26Bgps = ExtractBgps.getBgps(Algebra.compile(q26));
		//System.out.println(q10Bgps);
		QueryExecution q26Exec = QueryExecutionFactory.create(q26, model);
		ResultSet q26Results = q26Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q26Bgps.get(0), q26Results);
		
		
		String s27 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P279>  <http://www.wikidata.org/entity/Q33685>\n"
				+ "  }";
		
		Query q27 = QueryFactory.create(s27);
		myCache.cacheConstants(q27);
		SingleQuery sq27 = new SingleQuery(q27.toString(), true, true, false, true);
		q27 = QueryFactory.create(sq27.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q27Bgps = ExtractBgps.getBgps(Algebra.compile(q27));
		//System.out.println(q10Bgps);
		QueryExecution q27Exec = QueryExecutionFactory.create(q27, model);
		ResultSet q27Results = q27Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q27Bgps.get(0), q27Results);
		
		String s28 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { <http://www.wikidata.org/entity/Q501128> ?var1  ?v\n"
				+ "  }";
		
		Query q28 = QueryFactory.create(s28);
		myCache.cacheConstants(q28);
		SingleQuery sq28 = new SingleQuery(q28.toString(), true, true, false, true);
		q28 = QueryFactory.create(sq28.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q28Bgps = ExtractBgps.getBgps(Algebra.compile(q28));
		//System.out.println(q10Bgps);
		QueryExecution q28Exec = QueryExecutionFactory.create(q28, model);
		ResultSet q28Results = q28Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q28Bgps.get(0), q28Results);
		
		
		String s29 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P195>  <http://www.wikidata.org/entity/Q2817221>\n"
				+ "  }";
		
		Query q29 = QueryFactory.create(s29);
		myCache.cacheConstants(q29);
		SingleQuery sq29 = new SingleQuery(q29.toString(), true, true, false, true);
		q29 = QueryFactory.create(sq29.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q29Bgps = ExtractBgps.getBgps(Algebra.compile(q29));
		//System.out.println(q10Bgps);
		QueryExecution q29Exec = QueryExecutionFactory.create(q29, model);
		ResultSet q29Results = q29Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q29Bgps.get(0), q29Results);
		
		String s30 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { <http://www.wikidata.org/entity/Q620588> ?var1  ?v\n"
				+ "  }";
		
		Query q30 = QueryFactory.create(s30);
		myCache.cacheConstants(q30);
		SingleQuery sq30 = new SingleQuery(q30.toString(), true, true, false, true);
		q30 = QueryFactory.create(sq30.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q30Bgps = ExtractBgps.getBgps(Algebra.compile(q30));
		//System.out.println(q10Bgps);
		QueryExecution q30Exec = QueryExecutionFactory.create(q30, model);
		ResultSet q30Results = q30Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q30Bgps.get(0), q30Results);
		
		String s31 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { <http://www.wikidata.org/entity/Q1100609>  <http://www.wikidata.org/prop/direct/P69>  ?v\n"
				+ "  }";
		
		Query q31 = QueryFactory.create(s31);
		myCache.cacheConstants(q31);
		SingleQuery sq31 = new SingleQuery(q31.toString(), true, true, false, true);
		q31 = QueryFactory.create(sq31.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q31Bgps = ExtractBgps.getBgps(Algebra.compile(q31));
		//System.out.println(q10Bgps);
		QueryExecution q31Exec = QueryExecutionFactory.create(q31, model);
		ResultSet q31Results = q31Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q31Bgps.get(0), q31Results);
		
		String s32 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P629>  ?v\n"
				+ "  }";
		
		Query q32 = QueryFactory.create(s32);
		myCache.cacheConstants(q32);
		SingleQuery sq32 = new SingleQuery(q32.toString(), true, true, false, true);
		q32 = QueryFactory.create(sq32.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q32Bgps = ExtractBgps.getBgps(Algebra.compile(q32));
		//System.out.println(q10Bgps);
		QueryExecution q32Exec = QueryExecutionFactory.create(q32, model);
		ResultSet q32Results = q32Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q32Bgps.get(0), q32Results);
		
		String s33 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P2037>  ?v\n"
				+ "  }";
		
		Query q33 = QueryFactory.create(s33);
		myCache.cacheConstants(q33);
		SingleQuery sq33 = new SingleQuery(q33.toString(), true, true, false, true);
		q33 = QueryFactory.create(sq33.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q33Bgps = ExtractBgps.getBgps(Algebra.compile(q33));
		//System.out.println(q10Bgps);
		QueryExecution q33Exec = QueryExecutionFactory.create(q33, model);
		ResultSet q33Results = q33Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q33Bgps.get(0), q33Results);
		
		String s34 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P2984>  ?v\n"
				+ "  }";
		
		Query q34 = QueryFactory.create(s34);
		myCache.cacheConstants(q34);
		SingleQuery sq34 = new SingleQuery(q34.toString(), true, true, false, true);
		q34 = QueryFactory.create(sq34.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q34Bgps = ExtractBgps.getBgps(Algebra.compile(q34));
		//System.out.println(q10Bgps);
		QueryExecution q34Exec = QueryExecutionFactory.create(q34, model);
		ResultSet q34Results = q34Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q34Bgps.get(0), q34Results);
		
		String s35 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P170>  <http://www.wikidata.org/entity/Q5582>\n"
				+ "  }";
		
		Query q35 = QueryFactory.create(s35);
		myCache.cacheConstants(q35);
		SingleQuery sq35 = new SingleQuery(q35.toString(), true, true, false, true);
		q35 = QueryFactory.create(sq35.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q35Bgps = ExtractBgps.getBgps(Algebra.compile(q35));
		//System.out.println(q10Bgps);
		QueryExecution q35Exec = QueryExecutionFactory.create(q35, model);
		ResultSet q35Results = q35Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q35Bgps.get(0), q35Results);
		
		String s36 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P509>  ?v\n"
				+ "  }";
		
		Query q36 = QueryFactory.create(s36);
		myCache.cacheConstants(q36);
		SingleQuery sq36 = new SingleQuery(q36.toString(), true, true, false, true);
		q36 = QueryFactory.create(sq36.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q36Bgps = ExtractBgps.getBgps(Algebra.compile(q36));
		//System.out.println(q10Bgps);
		QueryExecution q36Exec = QueryExecutionFactory.create(q36, model);
		ResultSet q36Results = q36Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q36Bgps.get(0), q36Results);
		
		String s37 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P245>  ?v\n"
				+ "  }";
		
		Query q37 = QueryFactory.create(s37);
		myCache.cacheConstants(q37);
		SingleQuery sq37 = new SingleQuery(q37.toString(), true, true, false, true);
		q37 = QueryFactory.create(sq37.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q37Bgps = ExtractBgps.getBgps(Algebra.compile(q37));
		//System.out.println(q10Bgps);
		QueryExecution q37Exec = QueryExecutionFactory.create(q37, model);
		ResultSet q37Results = q37Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q37Bgps.get(0), q37Results);
		
		String s38 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q2221906>\n"
				+ "  }";
		
		Query q38 = QueryFactory.create(s38);
		myCache.cacheConstants(q38);
		SingleQuery sq38 = new SingleQuery(q38.toString(), true, true, false, true);
		q38 = QueryFactory.create(sq38.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q38Bgps = ExtractBgps.getBgps(Algebra.compile(q38));
		//System.out.println(q10Bgps);
		QueryExecution q38Exec = QueryExecutionFactory.create(q38, model);
		ResultSet q38Results = q38Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q38Bgps.get(0), q38Results);
		
		String s39 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P3329>  ?v\n"
				+ "  }";
		
		Query q39 = QueryFactory.create(s39);
		myCache.cacheConstants(q39);
		SingleQuery sq39 = new SingleQuery(q39.toString(), true, true, false, true);
		q39 = QueryFactory.create(sq39.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q39Bgps = ExtractBgps.getBgps(Algebra.compile(q39));
		//System.out.println(q10Bgps);
		QueryExecution q39Exec = QueryExecutionFactory.create(q39, model);
		ResultSet q39Results = q39Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q39Bgps.get(0), q39Results);
		
		String s40 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P2410>  ?v\n"
				+ "  }";
		
		Query q40 = QueryFactory.create(s40);
		myCache.cacheConstants(q40);
		SingleQuery sq40 = new SingleQuery(q40.toString(), true, true, false, true);
		q40 = QueryFactory.create(sq40.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q40Bgps = ExtractBgps.getBgps(Algebra.compile(q40));
		//System.out.println(q10Bgps);
		QueryExecution q40Exec = QueryExecutionFactory.create(q40, model);
		ResultSet q40Results = q40Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q40Bgps.get(0), q40Results);
		
		String s41 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P106>  <http://www.wikidata.org/entity/Q82594>\n"
				+ "  }";
		
		Query q41 = QueryFactory.create(s41);
		myCache.cacheConstants(q41);
		SingleQuery sq41 = new SingleQuery(q41.toString(), true, true, false, true);
		q41 = QueryFactory.create(sq41.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q41Bgps = ExtractBgps.getBgps(Algebra.compile(q41));
		//System.out.println(q10Bgps);
		QueryExecution q41Exec = QueryExecutionFactory.create(q41, model);
		ResultSet q41Results = q41Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q41Bgps.get(0), q41Results);
		
		String s42 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P2601>  ?v\n"
				+ "  }";
		
		Query q42 = QueryFactory.create(s42);
		myCache.cacheConstants(q42);
		SingleQuery sq42 = new SingleQuery(q42.toString(), true, true, false, true);
		q42 = QueryFactory.create(sq42.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q42Bgps = ExtractBgps.getBgps(Algebra.compile(q42));
		//System.out.println(q10Bgps);
		QueryExecution q42Exec = QueryExecutionFactory.create(q42, model);
		ResultSet q42Results = q42Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q42Bgps.get(0), q42Results);
		
		String s43 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q431289>\n"
				+ "  }";
		
		Query q43 = QueryFactory.create(s43);
		myCache.cacheConstants(q43);
		SingleQuery sq43 = new SingleQuery(q43.toString(), true, true, false, true);
		q43 = QueryFactory.create(sq43.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q43Bgps = ExtractBgps.getBgps(Algebra.compile(q43));
		//System.out.println(q10Bgps);
		QueryExecution q43Exec = QueryExecutionFactory.create(q43, model);
		ResultSet q43Results = q43Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q43Bgps.get(0), q43Results);
		
		String s44 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q9788>\n"
				+ "  }";
		
		Query q44 = QueryFactory.create(s44);
		myCache.cacheConstants(q44);
		SingleQuery sq44 = new SingleQuery(q44.toString(), true, true, false, true);
		q44 = QueryFactory.create(sq44.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q44Bgps = ExtractBgps.getBgps(Algebra.compile(q44));
		//System.out.println(q10Bgps);
		QueryExecution q44Exec = QueryExecutionFactory.create(q44, model);
		ResultSet q44Results = q44Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q44Bgps.get(0), q44Results);
		
		String s45 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P551>  ?v\n"
				+ "  }";
		
		Query q45 = QueryFactory.create(s45);
		myCache.cacheConstants(q45);
		SingleQuery sq45 = new SingleQuery(q45.toString(), true, true, false, true);
		q45 = QueryFactory.create(sq45.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q45Bgps = ExtractBgps.getBgps(Algebra.compile(q45));
		//System.out.println(q10Bgps);
		QueryExecution q45Exec = QueryExecutionFactory.create(q45, model);
		ResultSet q45Results = q45Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q45Bgps.get(0), q45Results);
		
		String s46 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P771>  ?v\n"
				+ "  }";
		
		Query q46 = QueryFactory.create(s46);
		myCache.cacheConstants(q46);
		SingleQuery sq46 = new SingleQuery(q46.toString(), true, true, false, true);
		q46 = QueryFactory.create(sq46.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q46Bgps = ExtractBgps.getBgps(Algebra.compile(q46));
		//System.out.println(q10Bgps);
		QueryExecution q46Exec = QueryExecutionFactory.create(q46, model);
		ResultSet q46Results = q46Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q46Bgps.get(0), q46Results);
		
		String s47 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P30>  ?v\n"
				+ "  }";
		
		Query q47 = QueryFactory.create(s47);
		myCache.cacheConstants(q47);
		SingleQuery sq47 = new SingleQuery(q47.toString(), true, true, false, true);
		q47 = QueryFactory.create(sq47.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q47Bgps = ExtractBgps.getBgps(Algebra.compile(q47));
		//System.out.println(q10Bgps);
		QueryExecution q47Exec = QueryExecutionFactory.create(q47, model);
		ResultSet q47Results = q47Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q47Bgps.get(0), q47Results);
		
		String s48 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P279>  <http://www.wikidata.org/entity/Q5398426>\n"
				+ "  }";
		
		Query q48 = QueryFactory.create(s48);
		myCache.cacheConstants(q48);
		SingleQuery sq48 = new SingleQuery(q48.toString(), true, true, false, true);
		q48 = QueryFactory.create(sq48.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q48Bgps = ExtractBgps.getBgps(Algebra.compile(q48));
		//System.out.println(q10Bgps);
		QueryExecution q48Exec = QueryExecutionFactory.create(q48, model);
		ResultSet q48Results = q48Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q48Bgps.get(0), q48Results);
		
		String s49 = "PREFIX wiki: <http://www.wikidata.org/prop/direct/>\n"
				+ "PREFIX we: <http://www.wikidata.org/entity/>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX schema: <http://schema.org/>\n"
				+ "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P1733>  ?v\n"
				+ "  }";
		
		Query q49 = QueryFactory.create(s49);
		myCache.cacheConstants(q49);
		SingleQuery sq49 = new SingleQuery(q49.toString(), true, true, false, true);
		q49 = QueryFactory.create(sq49.getQuery(), Syntax.syntaxARQ);
		ArrayList<OpBGP> q49Bgps = ExtractBgps.getBgps(Algebra.compile(q49));
		//System.out.println(q10Bgps);
		QueryExecution q49Exec = QueryExecutionFactory.create(q49, model);
		ResultSet q49Results = q49Exec.execSelect();
		//ResultSetMem r = new ResultSetMem(q11Results);
		//System.out.println(r.size());
		//System.out.println(ResultSetFormatter.asText(q10Results));
		myCache.cache(q49Bgps.get(0), q49Results);
		
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
		
		
		final BufferedReader tsv = 
				new BufferedReader (
						new InputStreamReader(
								new GZIPInputStream(
										new FileInputStream(
												new File("D:\\wikidata_logs\\2017-07-10_2017-08-06_organic.tsv.gz")))));
		
		/*final BufferedReader tsv = 
				new BufferedReader (
						new InputStreamReader(
								new GZIPInputStream(
										new FileInputStream(
												new File("D:\\wikidata_logs\\five_queries.tsv.gz")))));
		
		final BufferedReader tsv = 
				new BufferedReader (
						new InputStreamReader(
								new GZIPInputStream(
										new FileInputStream(
												new File("D:\\wikidata_logs\\five_queries_nc.tsv.gz")))));
		*/
		//System.out.println(getNumberOfCompressedLines("D:\\wikidata_logs\\five_queries.tsv.gz"));
		//System.out.println(getNumberOfCompressedLines("D:\\wikidata_logs\\five_queries_nc.tsv.gz"));
		System.out.println(getNumberOfCompressedLines("D:\\wikidata_logs\\2017-07-10_2017-08-06_organic.tsv.gz"));
		
		final PrintWriter w = new PrintWriter(new FileWriter("D:\\tmp\\CacheQueriesV2.txt"));
		
		// Only if first line is garbage
		//tsv.readLine();
		
		for (int i = 1; i <= 5000; i++) {
			try {
				System.out.println("Reading query " + j++);
				String line = tsv.readLine();
						
				long startLine = System.nanoTime();
						
				Parser parser = new Parser();
				Query q = parser.parseDbPedia(line);
						
				long afterParse = System.nanoTime();
				String ap = "Time to parse: " + (afterParse - startLine);
						
				Op inputOp = Algebra.compile(q);
						
				Transform cacheTransform = new CacheTransformCopy(myCache, startLine);
				Op cachedOp = Transformer.transform(cacheTransform, inputOp);
						
				String solution = ((CacheTransformCopy) cacheTransform).getSolution();
				long beforeOptimize = System.nanoTime();
				String bo = "Time before optimizing: " + (beforeOptimize - startLine);
				
				Op opjoin = Algebra.optimize(cachedOp);
				Query query = OpAsQuery.asQuery(opjoin);
				QueryExecution qExec = QueryExecutionFactory.create(query, model);
				qExec.setTimeout(1, TimeUnit.MINUTES, 1, TimeUnit.MINUTES);
				
				long start = System.nanoTime();
				String br = "Time before reading results: " + (start - startLine);
				
				//QueryIterator cache_qit = Algebra.exec(opjoin, model);
				ResultSet cache_qit = qExec.execSelect();
				System.out.println("Read Query!");
				int cacheResultAmount = 0;
				
				while (cache_qit.hasNext()) {
					cache_qit.next();
					cacheResultAmount++;
				}
				
				long stop = System.nanoTime();
				String ar = "Time after reading all results: " + (stop - startLine);
				//TimeUnit.MINUTES.sleep(1);
				
				if (cacheResultAmount != 0) {
					System.out.println("FOUND ONE");
					w.println("Info for query number " + (j-1));
					w.println(q);
					w.println(ap);
					w.println(solution);
					w.println(bo);
					w.println(br);
					w.println(ar);
					w.println("Query " + (j-1) + " Results with cache: " + cacheResultAmount);
					w.println("");
				}
			} catch (Exception e) {}
		};
	w.close();
	}
}
