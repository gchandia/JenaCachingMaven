package cl.uchile.dcc.caching.utils;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.tdb.TDBFactory;

import cl.uchile.dcc.caching.bgps.ExtractBgps;
import cl.uchile.dcc.caching.cache.Cache;
import cl.uchile.dcc.caching.cache.CustomCacheV5;
import cl.uchile.dcc.caching.common_joins.Parser;

public class HelloWorld {
  public static void main(String[] args) throws Exception {
	System.out.println("Hello World");
	String s = "SELECT  ?var1\r\n"
			+ "WHERE\r\n"
			+ "  { ?var1  <http://www.wikidata.org/prop/direct/P138>  <http://www.wikidata.org/entity/Q676555> }";
	Parser p = new Parser();
	Query q = p.parseDbPedia(s);
    ArrayList<OpBGP> bgps = ExtractBgps.getBgps(Algebra.optimize(Algebra.compile(q)));
    System.out.println(bgps.get(0));
    String myModel = "D:\\tmp\\WikiDB";
    Dataset ds = TDBFactory.createDataset(myModel);
    ds.begin(ReadWrite.READ);
    Model model = ds.getDefaultModel();
    QueryExecution qExec = QueryExecutionFactory.create(q, model);
    QueryExecution qExecTwo = QueryExecutionFactory.create(q, model);
    ResultSet qResults = qExec.execSelect();
    ResultSet qResultsTwo = qExecTwo.execSelect();
    System.out.println(qResultsTwo.hasNext());
    int ra = 0;
    while(qResultsTwo.hasNext()) {
      qResultsTwo.next();
      ra++;
    }
    System.out.println(ra);
    Cache c = new CustomCacheV5(1000, 10000000, 900, 10);
    c.cache(bgps.get(0), qResults);
    Iterator<Triple> it = bgps.get(0).getPattern().iterator();
    PrintWriter w = new PrintWriter(new FileWriter("D:\\Thesis\\Cache.tsv"));
    while (it.hasNext()) {
      w.print(it.next().toString() + '\t');
    }
    
    w.close();
  }
}
