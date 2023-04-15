package cl.uchile.dcc.caching.cache;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.tdb.TDBFactory;

import cl.uchile.dcc.caching.bgps.ExtractBgps;
import cl.uchile.dcc.caching.common_joins.Parser;

class Block {
  private boolean lir;
  private boolean resident;
  
  Block(boolean lir) {
	this.lir = lir;
	this.resident = true;
  }
  
  public boolean isLir() {
	return this.lir;
  }
  
  public boolean isResident() {
	return this.resident;
  }
  
  public void setLir(boolean lir) {
	this.lir = lir;
  }
  
  public void setResident(boolean resident) {
	this.resident = resident;
  }
}

public class LIRSCacheV3 extends AbstractCache {
  //S contains LIR, resident and non-resident HIR blocks
  //Index 0 represent the bottom, whereas the last index represents the top
  private LinkedHashMap<OpBGP, Block> stackS;
  //Q contains only resident HIR blocks
  //Index 0 represents the front, whereas the last index represents the end
  private LinkedHashMap<OpBGP, Block> listQ;
  private int stackLimit;
  private int queueLimit;
  
  public LIRSCacheV3(int itemLimit, int resultsLimit, int stackLimit, int queueLimit) {
	super(itemLimit, resultsLimit);
	this.stackS = new LinkedHashMap<OpBGP, Block>();
	this.listQ = new LinkedHashMap<OpBGP, Block>();
	this.stackLimit = stackLimit;
	this.queueLimit = queueLimit;
  }
  
  private void pruneStack() {
	while (!this.stackS.get(this.stackS.keySet().iterator().next()).isLir()) {
	  this.stackS.remove(this.stackS.keySet().iterator().next());
	}
  }
  
  private void cycle() {
	if (listQ.size() >= queueLimit) {
      removeFromCache();
	}
	Entry<OpBGP, Block> y = this.stackS.entrySet().iterator().next();
	this.stackS.remove(y.getKey());
	pruneStack();
	this.listQ.put(y.getKey(), y.getValue());
	//Since it moves to q it has to be HIR
	y.getValue().setLir(false);
  }
  
  @Override
  protected boolean addToCache(OpBGP bgp, OpTable opt) {
	if (this.queryToSolution.get(bgp) == null) {  //Case 1 k doesn't belong in the cache
	  if (this.stackS.size() < this.stackLimit) { //Case 1.1 |LIRS| < l
		 this.stackS.put(bgp, new Block(true));
	  } else if (this.stackS.get(bgp) == null) {  //Case 1.2 k doesn't belong to the stack
		  //If S is full new block enters as HIR
		  this.stackS.put(bgp, new Block(false));
		  if (listQ.size() >= queueLimit) {
			removeFromCache();
		  }
		  this.listQ.put(bgp, this.stackS.get(bgp));
	  } else if (this.stackS.get(bgp) != null) {  //Case 1.3 k belongs to the stack
		  Block v = this.stackS.get(bgp);
		  this.stackS.remove(bgp);
		  this.stackS.put(bgp, v);
		  //Promote to a hot block?
		  v.setResident(true);
		  v.setLir(true);
		  cycle();
	  }
	  queryToSolution.put(bgp, opt);
	  return true;
	}
	//Case 2 k already belongs in the cache
	if (this.stackS.get(bgp) != null && this.listQ.get(bgp) == null) { //Case 2.1 k belongs to the stack and doesn't belong to the list
		Block v = this.stackS.get(bgp);
		this.stackS.remove(bgp);
		this.stackS.put(bgp, v);
		//Promote to a hot block?
		v.setResident(true);
		v.setLir(true);
		pruneStack();
	} else if (this.stackS.get(bgp) != null && this.listQ.get(bgp) != null) { //Case 2.2 k belongs to both the stack and the list
		Block v = this.stackS.get(bgp);
		this.stackS.remove(bgp);
		this.stackS.put(bgp, v);
		//Promote to a hot block?
		v.setResident(true);
		v.setLir(true);
		OpBGP k = this.listQ.keySet().iterator().next();
		this.listQ.remove(k);
		cycle();
	} else if (this.stackS.get(bgp) == null && this.listQ.get(bgp) != null) { //Case 2.3 k doesn't belong to the stack and belongs to the list
		this.stackS.put(bgp, new Block(false));
		Block v = this.listQ.get(bgp);
		this.listQ.remove(bgp);
		this.listQ.put(bgp, v);
	}
	return false;
  }
  
  @Override
  public ArrayList<Op> retrieveCache(ArrayList<Op> input, 
	 							   	 OpBGP ret, 
									 ArrayList<OpBGP> bgpList,
									 Map<String, String> varMap, 
									 long startLine) {
	//Call addToCache, it will go to the case where it belongs to the cache
	addToCache(ret, null);
	return super.retrieveCache(input, ret, bgpList, varMap, startLine);
  }
  
  @Override
  protected void removeFromCache() {
	//We search the first resident HIRS block in list Q and remove it from the cache and the list
	OpBGP LIRSKey = listQ.keySet().iterator().next();
	queryToSolution.remove(LIRSKey);
	listQ.remove(LIRSKey);
	//We change the status of the block X if found in the stack S
	Block s = stackS.get(LIRSKey);
	if (s != null) {
	  s.setResident(false);
	  s.setLir(false);
	}
	//We then remove the constants from the cache
	Query qu = formQuery(LIRSKey);
	removeConstants(qu);
  }
  
  public LinkedHashMap<OpBGP, Block> getStack() {
	return this.stackS;
  }
		  
  public LinkedHashMap<OpBGP, Block> getList() {
	return this.listQ;
  }
  
  public HashMap<OpBGP, OpTable> getQueryToSolution() {
	return this.queryToSolution;
  }
  
  public static void main(String[] args) {
	String myModel = "D:\\tmp\\WikiDB";
	Dataset ds = TDBFactory.createDataset(myModel);
	ds.begin(ReadWrite.READ);
	Model model = ds.getDefaultModel();
	LIRSCacheV3 c = new LIRSCacheV3(10, 10000, 2, 1);
	
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
	
	//True caching testing
	//Blocks are accessed in the following order: A-D-B-C-B-A-D-A-E
	c.cache(bgpA, qRA);
	c.cache(bgpD, qRD);
	c.cache(bgpB, qRB);
	c.cache(bgpC, qRC);
	qExecB = QueryExecutionFactory.create(queryB, model);
	qRB = qExecB.execSelect();
	c.cache(bgpB, qRB);
	qExecA = QueryExecutionFactory.create(queryA, model);
	qRA = qExecA.execSelect();
	c.cache(bgpA, qRA);
	qExecD = QueryExecutionFactory.create(queryD, model);
	qRD = qExecD.execSelect();
	c.cache(bgpD, qRD);
	qExecA = QueryExecutionFactory.create(queryA, model);
	qRA = qExecA.execSelect();
	c.cache(bgpA, qRA);
	c.cache(bgpE, qRE);
	
	Iterator<OpBGP> it = c.getStack().keySet().iterator();
	Iterator<OpBGP> itt = c.getList().keySet().iterator();
	Iterator<OpBGP> cit = c.getQueryToSolution().keySet().iterator();
	
	OpBGP entrySD = it.next();
	System.out.println(entrySD + ", LIR: " + c.getStack().get(entrySD).isLir() + ", resident: " + c.getStack().get(entrySD).isResident());
	OpBGP entrySA = it.next();
	System.out.println(entrySA + ", LIR: " + c.getStack().get(entrySA).isLir() + ", resident: " + c.getStack().get(entrySA).isResident());
	OpBGP entrySE = it.next();
	System.out.println(entrySE + ", LIR: " + c.getStack().get(entrySE).isLir() + ", resident: " + c.getStack().get(entrySE).isResident());
	OpBGP entryLE = itt.next();
	System.out.println(entryLE + ", LIR: " + c.getList().get(entryLE).isLir() + ", resident: " + c.getList().get(entryLE).isResident());
	System.out.println(cit.next() + " " + cit.next() + " " + cit.next());
  }
}
