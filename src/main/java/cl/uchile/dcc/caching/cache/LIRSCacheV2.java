package cl.uchile.dcc.caching.cache;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
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

public class LIRSCacheV2 extends AbstractCache {
  //S contains LIR, resident and non-resident HIR blocks
  //Index 0 represent the bottom, whereas the last index represents the top
  private LinkedHashMap<OpBGP, LIRSStruct> stackS;
  //Q contains only resident HIR blocks
  //Index 0 represents the front, whereas the last index represents the end
  private LinkedHashMap<OpBGP, LIRSStruct> listQ;
  //This helps calculate recency and IRR, introduces order in which blocks arrive
  private ArrayList<LIRSStruct> blocks;
  //Boolean to check if we're in the beginning state of the caching
  //This is, if we're only introducing lirs and hirs blocks
  private boolean beginning = true;
  
  public LIRSCacheV2(int itemLimit, int resultsLimit) {
	super(itemLimit, resultsLimit);
	this.stackS = new LinkedHashMap<OpBGP, LIRSStruct>();
	this.listQ = new LinkedHashMap<OpBGP, LIRSStruct>();
	this.blocks = new ArrayList<LIRSStruct>();
  }
  
  public LinkedHashMap<OpBGP, LIRSStruct> getStack() {
	return this.stackS;
  }
	  
  public LinkedHashMap<OpBGP, LIRSStruct> getList() {
	return this.listQ;
  }
	  
  public ArrayList<LIRSStruct> getBlocks() {
	return this.blocks;
  }
  
  private void updateRecency() {
	//Update recency of all items in both maps
	int total = this.blocks.size() - 1;
	for (LIRSStruct ls : blocks) {
	  ls.modifyRecency(total--);
	}
  }
  
  private void stackPruning() {
	//Remove all HIR blocks from the bottom of the stack
	while (!this.stackS.get(this.stackS.keySet().iterator().next()).isBlockLIR()) {
	  this.stackS.remove(this.stackS.keySet().iterator().next());
	}
  }
  
  private void updateBlock(OpBGP ret) {
    LIRSStruct s = this.stackS.get(ret);
	if (s != null) {
	  //We find a block in the stack, now we check if it is LIR or HIR
	  if (s.isBlockLIR()) {
	    //We found a LIR block in stack S
	    //We move the block to the top of the stack then do a stack pruning
	    this.stackS.remove(ret);
	    this.stackS.put(ret, s);
	    stackPruning();
	  } else {
	    //We find a HIR block in stack S
	    //We move it to the top of the stack
	    this.stackS.remove(ret);
	    this.stackS.put(ret, s);
	    s.setLIR(true);
	    //We also remove the block from list Q
	    this.listQ.remove(ret);
	    //We then remove the block at the bottom of S, move it to Q and change its status to HIR
	    Entry<OpBGP, LIRSStruct> e = this.stackS.entrySet().iterator().next();
	    this.stackS.remove(e.getKey());
	    this.listQ.put(e.getKey(), e.getValue());
	    e.getValue().setLIR(false);
	    //A stack pruning is then conducted
	    stackPruning();
	  }
	} else {
	  s = this.listQ.get(ret);
	  //We move the block to the end of the list
	  this.listQ.remove(ret);
	  this.listQ.put(ret, s);
	}
	
	//If the block is still not found it's a new block we put in top of the stack and of the list
	if (s == null) {
	  removeFromCache();
	  s = new LIRSStruct(false);
	  this.stackS.put(ret, s);
	  this.listQ.put(ret, s);
	}
	
	int index = this.blocks.indexOf(s);
	if (index != -1) s.setIRR(this.blocks.size() - index - 1);
	this.blocks.remove(s);
	this.blocks.add(s);
	updateRecency();
  }
  
  @Override
  protected boolean addToCache(OpBGP bgp, OpTable opt) {
	//In case block is not in the cache, we either add it to S as a LIR block or to Q as a HIR block
	LIRSStruct s = null;
	if (this.queryToSolution.get(bgp) == null) {
	  if (beginning) {
		//Check if we're in the first 99%, L_lirs
		s = this.stackS.get(bgp);
		if (s != null) {
		  this.stackS.remove(bgp);
		  this.stackS.put(bgp, s);
		  int index = this.blocks.indexOf(s);
		  s.setIRR(this.blocks.size() - index - 1);
		  this.blocks.remove(s);
		  this.blocks.add(s);
		  updateRecency();
		  this.queryToSolution.put(bgp, opt);
		  return false;
		}
		s = this.listQ.get(bgp);
		if (s != null) {
		  this.listQ.remove(bgp);
		  this.listQ.put(bgp, s);
		  int index = this.blocks.indexOf(s);
		  s.setIRR(this.blocks.size() - index - 1);
		  this.blocks.remove(s);
		  this.blocks.add(s);
		  updateRecency();
		  this.queryToSolution.put(bgp, opt);
		  return false;
		}
		if (resultsSize() < 99 * this.resultsLimit / 100) {
		  //We add the new block as a LIR block
		  s = new LIRSStruct(true);
		  this.stackS.put(bgp, s);
		  this.blocks.add(s);
		  updateRecency();
		  this.queryToSolution.put(bgp, opt);
		  return true;
		} else {
		  s = new LIRSStruct(false);
		  this.listQ.put(bgp, s);
		  this.blocks.add(s);
		  updateRecency();
		  this.queryToSolution.put(bgp, opt);
		  return true;
		}
	  }
	  //If not in a beginning status
	  //Check if block is non-resident in stack S
	  s = this.stackS.get(bgp);
	  if (s != null) {
		//If found in stack S as a non-resident block, we change its status to resident and to LIR
		//First we remove the first block from Q
		this.removeFromCache();
		s.setLIR(true);
		s.setResidency(true);
		//We get the block at the bottom of the stack
		Entry<OpBGP, LIRSStruct> e = this.stackS.entrySet().iterator().next();
		this.stackS.remove(e.getKey());
		//Then move it to Q with status HIR
		this.listQ.put(e.getKey(), e.getValue());
		e.getValue().setLIR(false);
		stackPruning();
		int index = this.blocks.indexOf(s);
		s.setIRR(this.blocks.size() - index - 1);
		this.blocks.remove(s);
		this.blocks.add(s);
		updateRecency();
	  } else {
		//If it's not in S, we move the block to the end of list Q
		s = new LIRSStruct(false);
		this.listQ.put(bgp, s);
		this.blocks.add(s);
		updateRecency();
	  }
	  //Cache the op
	  this.queryToSolution.put(bgp, opt);
	  return true;
	}
	//If block is already in the cache, we follow the heuristics in retrieve from cache
	updateBlock(bgp);
	return false;
  }
  
  @Override
  public ArrayList<Op> retrieveCache(ArrayList<Op> input, 
	 							   	 OpBGP ret, 
									 ArrayList<OpBGP> bgpList,
									 Map<String, String> varMap, 
									 long startLine) {
	//Follow heuristics
	updateBlock(ret);
	return super.retrieveCache(input, ret, bgpList, varMap, startLine);
  }
  
  @Override
  protected void removeFromCache() {
	//We search the first resident HIRS block in list Q and remove it from the cache and the list
	OpBGP LIRSKey = listQ.keySet().iterator().next();
	queryToSolution.remove(LIRSKey);
	listQ.remove(LIRSKey);
	//We change the status of the block X if found in the stack S
	LIRSStruct s = stackS.get(LIRSKey);
	if (s != null) s.setResidency(false);
	//We then remove the constants from the cache
	Query qu = formQuery(LIRSKey);
    removeConstants(qu);
    //After the first removal, the cache is full and we're past the beginning stage of the cache
    this.beginning = false;
  }
  
  //For testing purposes only
  public void putBlockInStack(OpBGP bgp, boolean LIR, boolean residency) {
	LIRSStruct s = new LIRSStruct(LIR);
	s.setResidency(residency);
	int index = this.blocks.indexOf(s);
	if (index != -1) s.setIRR(this.blocks.size() - index - 1);
	this.blocks.remove(s);
	this.blocks.add(s);
	updateRecency();
	this.stackS.put(bgp, s);
  }
  
  public void putBlockInList(OpBGP bgp, boolean LIR, boolean residency) {
	LIRSStruct s = new LIRSStruct(LIR);
	s.setResidency(residency);
	int index = this.blocks.indexOf(s);
	if (index != -1) s.setIRR(this.blocks.size() - index - 1);
	this.blocks.remove(s);
	this.blocks.add(s);
	updateRecency();
	this.listQ.put(bgp, s);
  }
  
  public void testCache(OpBGP bgp) {
	//In case block is not in the cache, we either add it to S as a LIR block or to Q as a HIR block
	LIRSStruct s = null;
	
	//Check if block is non-resident in stack S
	s = this.stackS.get(bgp);
	if (s != null && !s.isBlockResident()) {
	  //If found in stack S as a non-resident block, we change its status to resident and to LIR
	  //First we remove the first block from Q
	  this.removeFromCache();
	  s.setLIR(true);
	  s.setResidency(true);
	  //We get the block at the bottom of the stack
	  Entry<OpBGP, LIRSStruct> e = this.stackS.entrySet().iterator().next();
	  this.stackS.remove(e.getKey());
	  //Then move it to Q with status HIR
	  this.listQ.put(e.getKey(), e.getValue());
	  e.getValue().setLIR(false);
	  stackPruning();
	  int index = this.blocks.indexOf(s);
	  s.setIRR(this.blocks.size() - index - 1);
	  this.blocks.remove(s);
	  this.blocks.add(s);
	  updateRecency();
	}
	s = this.listQ.get(bgp);
	if (s != null && !s.isBlockResident()) {
	  //If it's not in S, we move the block to the end of list Q
	  s = new LIRSStruct(false);
	  this.listQ.put(bgp, s);
	  this.blocks.add(s);
	  updateRecency();
	} else {
	  //If block is already in the cache, we follow the heuristics in retrieve from cache
      updateBlock(bgp);
	}
  }
  
  public static void main(String[] args) {
	String myModel = "D:\\tmp\\WikiDB";
	Dataset ds = TDBFactory.createDataset(myModel);
	ds.begin(ReadWrite.READ);
	Model model = ds.getDefaultModel();
	LIRSCacheV2 c = new LIRSCacheV2(Integer.MAX_VALUE, 5);
	
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
	
	//Should have LIR block A in stack S, Q empty
	c.cache(bgpA, qRA);
	
	Iterator<OpBGP> it = c.getStack().keySet().iterator();
	Iterator<OpBGP> itt = c.getList().keySet().iterator();
	
	OpBGP entrySA = it.next();
	System.out.println(entrySA + ", LIR: " + c.getStack().get(entrySA).isBlockLIR() + ", resident: " + c.getStack().get(entrySA).isBlockResident());
	
	//Should have LIR blocks A and D, Q empty
	c.cache(bgpD, qRD);
	
	it = c.getStack().keySet().iterator();
	
	entrySA = it.next();
	System.out.println(entrySA + ", LIR: " + c.getStack().get(entrySA).isBlockLIR() + ", resident: " + c.getStack().get(entrySA).isBlockResident());
	
	OpBGP entrySD = it.next();
	System.out.println(entrySD + ", LIR: " + c.getStack().get(entrySD).isBlockLIR() + ", resident: " + c.getStack().get(entrySD).isBlockResident());
	
	//Should have LIR blocks A, D, B and C, Q empty
	c.cache(bgpB, qRB);
	c.cache(bgpC, qRC);
	
	it = c.getStack().keySet().iterator();
	
	entrySA = it.next();
	System.out.println(entrySA + ", LIR: " + c.getStack().get(entrySA).isBlockLIR() + ", resident: " + c.getStack().get(entrySA).isBlockResident());
	
	entrySD = it.next();
	System.out.println(entrySD + ", LIR: " + c.getStack().get(entrySD).isBlockLIR() + ", resident: " + c.getStack().get(entrySD).isBlockResident());
	
	OpBGP entrySB = it.next();
	System.out.println(entrySB + ", LIR: " + c.getStack().get(entrySB).isBlockLIR() + ", resident: " + c.getStack().get(entrySB).isBlockResident());
	
	OpBGP entrySC = it.next();
	System.out.println(entrySC + ", LIR: " + c.getStack().get(entrySC).isBlockLIR() + ", resident: " + c.getStack().get(entrySC).isBlockResident());
	
	//We re-use the results of B
	qExecB = QueryExecutionFactory.create(queryB, model);
	qRB = qExecB.execSelect();
	
	c.cache(bgpB, qRB);
	
	it = c.getStack().keySet().iterator();
	itt = c.getList().keySet().iterator();
	
	entrySA = it.next();
	System.out.println(entrySA + ", LIR: " + c.getStack().get(entrySA).isBlockLIR() + ", resident: " + c.getStack().get(entrySA).isBlockResident());
	
	entrySD = it.next();
	System.out.println(entrySD + ", LIR: " + c.getStack().get(entrySD).isBlockLIR() + ", resident: " + c.getStack().get(entrySD).isBlockResident());
	
	entrySC = it.next();
	System.out.println(entrySC + ", LIR: " + c.getStack().get(entrySC).isBlockLIR() + ", resident: " + c.getStack().get(entrySC).isBlockResident());
	
	entrySB = it.next();
	System.out.println(entrySB + ", LIR: " + c.getStack().get(entrySB).isBlockLIR() + ", resident: " + c.getStack().get(entrySB).isBlockResident());
	
	c.cache(bgpE, qRE);
	
	it = c.getStack().keySet().iterator();
	itt = c.getList().keySet().iterator();
	
	//Is empty
	OpBGP entryNull = itt.next();
	System.out.println(entryNull + ", LIR: " + c.getList().get(entryNull).isBlockLIR() + ", resident: " + c.getList().get(entryNull).isBlockResident());
	
	//False caching testing
	/*
	c.putBlockInStack(bgpB, true, true);
	c.putBlockInStack(bgpD, false, false);
	c.putBlockInStack(bgpA, true, true);
	c.putBlockInStack(bgpE, false, true);
	c.putBlockInList(bgpE, false, true);
	
	Iterator<OpBGP> it = c.getStack().keySet().iterator();
	Iterator<OpBGP> itt = c.getList().keySet().iterator();
	
	OpBGP entrySB = it.next();
	System.out.println(entrySB + ", LIR: " + c.getStack().get(entrySB).isBlockLIR() + ", resident: " + c.getStack().get(entrySB).isBlockResident());
	
	OpBGP entrySD = it.next();
	System.out.println(entrySD + ", LIR: " + c.getStack().get(entrySD).isBlockLIR() + ", resident: " + c.getStack().get(entrySD).isBlockResident());
	
	OpBGP entrySA = it.next();
	System.out.println(entrySA + ", LIR: " + c.getStack().get(entrySA).isBlockLIR() + ", resident: " + c.getStack().get(entrySA).isBlockResident());
	
	OpBGP entrySE = it.next();
	System.out.println(entrySE + ", LIR: " + c.getStack().get(entrySE).isBlockLIR() + ", resident: " + c.getStack().get(entrySE).isBlockResident());
	
	OpBGP entryLE = itt.next();
	System.out.println(entryLE + ", LIR: " + c.getList().get(entryLE).isBlockLIR() + ", resident: " + c.getList().get(entryLE).isBlockResident());
	System.out.println("--------------------------------");
	*/
	//First case: page B is accessed. WORKS!
	/*
	c.testCache(bgpB);
	
	it = c.getStack().keySet().iterator();
	itt = c.getList().keySet().iterator();
	
	entrySA = it.next();
	System.out.println(entrySA + ", LIR: " + c.getStack().get(entrySA).isBlockLIR() + ", resident: " + c.getStack().get(entrySA).isBlockResident());
	
	entrySE = it.next();
	System.out.println(entrySE + ", LIR: " + c.getStack().get(entrySE).isBlockLIR() + ", resident: " + c.getStack().get(entrySE).isBlockResident());
	
	entrySB = it.next();
	System.out.println(entrySB + ", LIR: " + c.getStack().get(entrySB).isBlockLIR() + ", resident: " + c.getStack().get(entrySB).isBlockResident());
	
	entryLE = itt.next();
	System.out.println(entryLE + ", LIR: " + c.getList().get(entryLE).isBlockLIR() + ", resident: " + c.getList().get(entryLE).isBlockResident());
	*/
	
	//Second case: page E is accessed. WORKS!
	/*
	c.testCache(bgpE);
	
	it = c.getStack().keySet().iterator();
	itt = c.getList().keySet().iterator();
	
	entrySA = it.next();
	System.out.println(entrySA + ", LIR: " + c.getStack().get(entrySA).isBlockLIR() + ", resident: " + c.getStack().get(entrySA).isBlockResident());
	
	entrySE = it.next();
	System.out.println(entrySE + ", LIR: " + c.getStack().get(entrySE).isBlockLIR() + ", resident: " + c.getStack().get(entrySE).isBlockResident());
	
	OpBGP entryLB = itt.next();
	System.out.println(entryLB + ", LIR: " + c.getList().get(entryLB).isBlockLIR() + ", resident: " + c.getList().get(entryLB).isBlockResident());
	*/
	
	//Third case: page D is accessed. WORKS!
	/*
	c.testCache(bgpD);
	
	it = c.getStack().keySet().iterator();
	itt = c.getList().keySet().iterator();
	
	entrySA = it.next();
	System.out.println(entrySA + ", LIR: " + c.getStack().get(entrySA).isBlockLIR() + ", resident: " + c.getStack().get(entrySA).isBlockResident());
	
	entrySE = it.next();
	System.out.println(entrySE + ", LIR: " + c.getStack().get(entrySE).isBlockLIR() + ", resident: " + c.getStack().get(entrySE).isBlockResident());
	
	entrySD = it.next();
	System.out.println(entrySD + ", LIR: " + c.getStack().get(entrySD).isBlockLIR() + ", resident: " + c.getStack().get(entrySD).isBlockResident());
	
	OpBGP entryLB = itt.next();
	System.out.println(entryLB + ", LIR: " + c.getList().get(entryLB).isBlockLIR() + ", resident: " + c.getList().get(entryLB).isBlockResident());
	*/
	
	//Final case: page C is accessed. WORKS!
	/*
	c.testCache(bgpC);
	
	it = c.getStack().keySet().iterator();
	itt = c.getList().keySet().iterator();
	
	entrySB = it.next();
	System.out.println(entrySB + ", LIR: " + c.getStack().get(entrySB).isBlockLIR() + ", resident: " + c.getStack().get(entrySB).isBlockResident());
	
	entrySD = it.next();
	System.out.println(entrySD + ", LIR: " + c.getStack().get(entrySD).isBlockLIR() + ", resident: " + c.getStack().get(entrySD).isBlockResident());
	
	entrySA = it.next();
	System.out.println(entrySA + ", LIR: " + c.getStack().get(entrySA).isBlockLIR() + ", resident: " + c.getStack().get(entrySA).isBlockResident());
	
	entrySE = it.next();
	System.out.println(entrySE + ", LIR: " + c.getStack().get(entrySE).isBlockLIR() + ", resident: " + c.getStack().get(entrySE).isBlockResident());
	
	OpBGP entrySC = it.next();
	System.out.println(entrySC + ", LIR: " + c.getStack().get(entrySC).isBlockLIR() + ", resident: " + c.getStack().get(entrySC).isBlockResident());
	
	OpBGP entryLC = itt.next();
	System.out.println(entryLC + ", LIR: " + c.getList().get(entryLC).isBlockLIR() + ", resident: " + c.getList().get(entryLC).isBlockResident());
	*/
  }
}
