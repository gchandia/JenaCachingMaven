package cl.uchile.dcc.caching.cache;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
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

class Stack {
  //We map LIRSStruct as key as to allow duplicate OpBGPs
  private Map<LIRSStruct, OpBGP> connections;
  //Index 0 is bottom, last index is top
  private ArrayList<OpBGP> arr;
  
  public Stack() {
	this.connections = new LinkedHashMap<LIRSStruct, OpBGP>();
	this.arr = new ArrayList<OpBGP>();
  }
  
  public boolean isEmpty() {
	return arr.size() == 0? true : false;
  }
  
  public void push(OpBGP bgp, LIRSStruct struct) {
	connections.put(struct, bgp);
	arr.add(bgp);
  }
  
  private OpBGP pop() {
	OpBGP bgp = arr.get(arr.size() - 1);
	arr.remove(arr.size() - 1);
	return bgp;
  }
  
  private OpBGP popBottom() {
	OpBGP bgp = arr.get(0);
	arr.remove(0);
	return bgp;
  }
  
  public Entry<LIRSStruct, OpBGP> getEntry(OpBGP bgp) {
	Entry<LIRSStruct, OpBGP> e = null;
	for (Entry<LIRSStruct, OpBGP> entry : this.connections.entrySet()) {
	  if (bgp.equals(entry.getValue())) {
		e = entry;
	  }
	}
	return e;
  }
  
  public Entry<LIRSStruct, OpBGP> structPop() {
	OpBGP bgp = this.pop();
	Entry<LIRSStruct, OpBGP> e = getEntry(bgp);
	this.connections.remove(e.getKey());
	return e;
  }
  
  public Entry<LIRSStruct, OpBGP> structPopBottom() {
	OpBGP bgp = this.popBottom();
	Entry<LIRSStruct, OpBGP> e = getEntry(bgp);
	this.connections.remove(e.getKey());
	return e;
  }
  
  public OpBGP peek() {
	return arr.get(arr.size() - 1);
  }
  
  public OpBGP peekBottom() {
	return arr.get(0);
  }
  
  public LIRSStruct peekStruct() {
	OpBGP bgp = peek();
	Entry<LIRSStruct, OpBGP> e = getEntry(bgp);
	return e.getKey();
  }
  
  public LIRSStruct peekStructBottom() {
	OpBGP bgp = peekBottom();
	Entry<LIRSStruct, OpBGP> e = getEntry(bgp);
	return e.getKey();
  }
}

class LIRSStruct {
  //If true it is resident, if false it is non-resident
  private boolean isResident;
  //If true it is LIR, if false it is HIR
  private boolean isLIR;
  private int recency;
  private int IRR;
  
  public LIRSStruct(boolean isLIR) {
	this.isResident = true;
	this.isLIR = isLIR;
	this.recency = 0;
	//The first time a block is accessed its IRR will always be infinite
	this.IRR = Integer.MAX_VALUE;
  }
  
  public boolean isBlockResident() {
	return this.isResident;
  }
  
  public boolean isBlockLIR() {
	return this.isLIR;
  }
  
  public int getRecency() {
	return this.recency;
  }
  
  public int getIRR() {
	return this.IRR;
  }
  
  public void setResidency(boolean residency) {
	this.isResident = residency;
  }
  
  public void setLIR(boolean isLIR) {
	this.isLIR = isLIR;
  }
  
  public void modifyRecency(int recency) {
	this.recency = recency;
  }
  
  public void setIRR(int IRR) {
	this.IRR = IRR;
  }
}

public class LIRSCache extends AbstractCache {
  //S contains LIR, resident and non-resident HIR blocks
  //Index 0 represent the bottom, whereas
  private Stack stackS;
  //Q contains only resident HIR blocks
  //Index 0 represents the front, whereas the last index represents the end
  private LinkedHashMap<OpBGP, LIRSStruct> listQ;
  private ArrayList<LIRSStruct> blocks;
  
  public LIRSCache(int itemLimit, int resultsLimit) {
	super(itemLimit, resultsLimit);
	stackS = new Stack();
	listQ = new LinkedHashMap<OpBGP, LIRSStruct>();
	blocks = new ArrayList<LIRSStruct>();
  }
  
  public Stack getStack() {
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
	//Remove all non-resident blocks from the bottom of the stack
	while (!this.stackS.peekStructBottom().isBlockResident()) {
	  this.stackS.structPopBottom();
	}
  }
  
  private boolean isItemInStack(OpBGP bgp) {
	Stack clone = this.stackS;
	boolean flag = false;
	while (!this.stackS.isEmpty()) {
	  Entry<LIRSStruct, OpBGP> e = this.stackS.structPopBottom();
	  if (bgp.equals(e.getValue())) {
		flag = true;
		break;
	  }
	}
	this.stackS = clone;
	return flag;
  }
  
  private Entry<LIRSStruct, OpBGP> getItemInStack(OpBGP bgp) {
	Stack clone = this.stackS;
	Entry<LIRSStruct, OpBGP> output = null;
	while (!this.stackS.isEmpty()) {
	  Entry<LIRSStruct, OpBGP> e = this.stackS.structPopBottom();
	  if (bgp.equals(e.getValue())) {
		output = e;
	  }
	}
	this.stackS = clone;
	return output;
  }
  
  private void removeItemFromStack(OpBGP bgp) {
	Stack clone = new Stack();
	while (!this.stackS.isEmpty()) {
	  Entry<LIRSStruct, OpBGP> e = this.stackS.structPopBottom();
	  if (bgp.equals(e.getValue())) continue;
	  clone.push(e.getValue(), e.getKey());
	}
	this.stackS = clone;
  }
  
  @Override
  protected boolean addToCache(OpBGP bgp, OpTable opt) {
	if (this.queryToSolution.get(bgp) == null) {
	  this.queryToSolution.put(bgp, opt);
	  LIRSStruct lirs = null;
	  //If block X is in stack S, we set it as LIR and move the bottom of the stack to the end of list Q
	  if (isItemInStack(bgp)) {
		Entry<LIRSStruct, OpBGP> e = getItemInStack(bgp);
		lirs = e.getKey();
		e.getKey().setLIR(true);
		Entry<LIRSStruct, OpBGP> bottom = this.stackS.structPopBottom();
		this.listQ.put(bottom.getValue(), bottom.getKey());
		blocks.add(lirs);
		updateRecency();
		return true;
	  } else {
		//We insert a fresh block in list Q with HIR status
		lirs = new LIRSStruct(false);
		this.listQ.put(bgp, lirs);
		blocks.add(lirs);
		updateRecency();
		
	  }
	}
	return false;
  }
  
  @Override
  public ArrayList<Op> retrieveCache(ArrayList<Op> input, 
	 							   	 OpBGP ret, 
									 ArrayList<OpBGP> bgpList,
									 Map<String, String> varMap, 
									 long startLine) {
	LIRSStruct lirs = null;
	//We hit a block in the stack s
	if (isItemInStack(ret)) {
	  Entry<LIRSStruct, OpBGP> e = getItemInStack(ret);
	  lirs = e.getKey();
	  removeItemFromStack(ret);
	  this.stackS.push(ret, e.getKey());
	  
	  if (!e.getKey().isBlockLIR()) {
		listQ.remove(ret);
		//We get the first LIR entry in the stack, which is at the bottom
		Entry<LIRSStruct, OpBGP> bottomOfStack = this.stackS.structPopBottom();
		//We remove it from the stack S
		removeItemFromStack(bottomOfStack.getValue());
		//And move it to the end of list Q
		this.listQ.put(bottomOfStack.getValue(), bottomOfStack.getKey());
		//We then remove all non-resident blocks from the bottom of S
		stackPruning();
	  }
	  
	  //Either case if it is LIR or HIR, it will end up being a LIR block
	  e.getKey().setLIR(true);
	} else { //In this case the block we're looking for is in Q
	  lirs = this.listQ.get(ret);
	  //We remove it from wherever it is
	  this.listQ.remove(ret);
	  //And move it to the end
	  this.listQ.put(ret, lirs);
	}
	
	int index = blocks.indexOf(lirs);
	lirs.setIRR(blocks.size() - index - 1);
	blocks.remove(lirs);
	blocks.add(lirs);
	updateRecency();
	return super.retrieveCache(input, ret, bgpList, varMap, startLine);
  }
  
  @Override
  protected void removeFromCache() {
	//We search the first resident HIRS block in list Q and remove it from the cache
	OpBGP LIRSKey = listQ.keySet().iterator().next();
	queryToSolution.remove(LIRSKey);
	//We then change the residency status of the blocks in the stack if found
	Stack clone = this.stackS;
	while (!this.stackS.isEmpty()) {
	  Entry<LIRSStruct, OpBGP> e = this.stackS.structPopBottom();
	  if (LIRSKey.equals(e.getValue())) e.getKey().setResidency(false);
	  clone.push(e.getValue(), e.getKey());
	}
	this.stackS = clone;
	
	Query qu = formQuery(LIRSKey);
    removeConstants(qu);
  }
  
  public static void main(String[] args) {
	String myModel = "D:\\tmp\\WikiDB";
	Dataset ds = TDBFactory.createDataset(myModel);
	ds.begin(ReadWrite.READ);
	Model model = ds.getDefaultModel();
	LIRSCache c = new LIRSCache(100, 1000000);
	
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
	} catch (UnsupportedEncodingException e) {}
	
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
    
    LinkedHashMap<OpBGP, LIRSStruct> list = c.getList();
    Stack stack = c.getStack();
    
    System.out.println(list.keySet().iterator().next() + ", " + 
    				   list.values().iterator().next().getRecency() + ", " + 
    				   list.values().iterator().next().getIRR());
    System.out.println(stack.isEmpty());
  }
}
