package cl.uchile.dcc.caching.cache;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.tdb.TDBFactory;

import cl.uchile.dcc.caching.bgps.ExtractBgps;
import cl.uchile.dcc.caching.common_joins.Parser;

public class RRCache extends AbstractCache {
  private ArrayList<OpBGP> keys;
	
  public RRCache(int sizeLimit, int resultsLimit) {
	super(sizeLimit, resultsLimit);
	this.keys = new ArrayList<OpBGP>();
  }
  
  @Override
  protected boolean addToCache(OpBGP bgp, OpTable opt) {
	if (this.queryToSolution.get(bgp) == null) {
	  this.queryToSolution.put(bgp, opt);
	  this.keys.add(bgp);
	  return true;
	}
	return false;
  }
  
  private OpBGP searchRandomKey() {
    Random r = new Random();
	int removeIndex = r.nextInt(queryToSolution.size() + 1);
	return this.keys.remove(removeIndex);
  }
  
  @Override
  protected void removeFromCache() {
	OpBGP rrKey = searchRandomKey();
	queryToSolution.remove(rrKey);
	  
	Query qu = formQuery(rrKey);
	  
	removeConstants(qu);
  }
  
  public static void main(String[] args) {
	String myModel = "D:\\tmp\\WikiDB";
	Dataset ds = TDBFactory.createDataset(myModel);
	ds.begin(ReadWrite.READ);
	Model model = ds.getDefaultModel();
	RRCache c = new RRCache(100, 1000000);
	
	String blockA = "SELECT * WHERE { ?var1  <http://www.wikidata.org/prop/direct/P345>  \"tt0160330\" }";
	String blockB = "SELECT * WHERE { ?var1  <http://www.wikidata.org/prop/direct/P345>  \"tt0080163\" }";
	String blockC = "SELECT * WHERE { ?var1  <http://www.wikidata.org/prop/direct/P345>  \"tt2226333\" }";
	String blockD = "SELECT * WHERE { ?var1  <http://www.wikidata.org/prop/direct/P345>  \"tt0423842\" }";
	String blockE = "SELECT * WHERE { ?var1  <http://www.wikidata.org/prop/direct/P345>  \"tt1678042\" }";
	
	Parser p = new Parser();
		
	Query queryA = null;
	Query queryB = null;
	Query queryC = null;
	Query queryD = null;
	Query queryE = null;
		
	try {
	  queryA = p.parseDbPedia(blockA);
	  queryB = p.parseDbPedia(blockB);
	  queryC = p.parseDbPedia(blockC);
	  queryD = p.parseDbPedia(blockD);
	  queryE = p.parseDbPedia(blockE);
	} catch (UnsupportedEncodingException  e) {}
	
	OpBGP bgpA = ExtractBgps.getBgps(Algebra.compile(queryA)).get(0);
	OpBGP bgpB = ExtractBgps.getBgps(Algebra.compile(queryB)).get(0);
	OpBGP bgpC = ExtractBgps.getBgps(Algebra.compile(queryC)).get(0);
	OpBGP bgpD = ExtractBgps.getBgps(Algebra.compile(queryD)).get(0);
	OpBGP bgpE = ExtractBgps.getBgps(Algebra.compile(queryE)).get(0);
		
	QueryExecution qExecA = QueryExecutionFactory.create(queryA, model);
	ResultSet qRA = qExecA.execSelect();
	
	QueryExecution qExecB = QueryExecutionFactory.create(queryB, model);
	ResultSet qRB = qExecB.execSelect();
	    
	QueryExecution qExecC = QueryExecutionFactory.create(queryC, model);
	ResultSet qRC = qExecC.execSelect();
	    
	QueryExecution qExecD = QueryExecutionFactory.create(queryD, model);
	ResultSet qRD = qExecD.execSelect();
	    
	QueryExecution qExecE = QueryExecutionFactory.create(queryE, model);
	ResultSet qRE = qExecE.execSelect();
	
	c.cache(bgpA, qRA);
	c.cache(bgpB, qRB);
	c.cache(bgpC, qRC);
	c.cache(bgpD, qRD);
	c.cache(bgpE, qRE);
	
	System.out.println(c.getKeys());
	
	c.removeFromCache();
	
	System.out.println(c.getKeys());
  }
}
