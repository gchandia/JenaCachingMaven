package cl.uchile.dcc.caching.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTable;

class CustomBlock {
  private boolean lir;
  private boolean resident;
  private int results;
  
  CustomBlock(boolean lir, int results) {
    this.lir = lir;
    this.resident = true;
    this.results = results;
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
  
  public int getResults() {
	return this.results;
  }
}

public class CustomCache extends AbstractCache {
  //S contains LIR, resident and non-resident HIR blocks
  //Index 0 represent the bottom, whereas the last index represents the top
  private LinkedHashMap<OpBGP, CustomBlock> stackS;
  //Q contains only resident HIR blocks
  //Index 0 represents the front, whereas the last index represents the end
  private LinkedHashMap<OpBGP, CustomBlock> listQ;
  private int stackLimit;
  private int queueLimit;
  
  public CustomCache(int itemLimit, int resultsLimit, int stackLimit, int queueLimit) {
	super(itemLimit, resultsLimit);
	this.stackS = new LinkedHashMap<OpBGP, CustomBlock>();
	this.listQ = new LinkedHashMap<OpBGP, CustomBlock>();
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
	Entry<OpBGP, CustomBlock> y = this.stackS.entrySet().iterator().next();
	this.stackS.remove(y.getKey());
	pruneStack();
	this.listQ.put(y.getKey(), y.getValue());
	//Since it moves to q it has to be HIR
	y.getValue().setLir(false);
  }
  
  Comparator<Entry<OpBGP, CustomBlock>> resultsComparator = new Comparator<Entry<OpBGP,CustomBlock>>() { 
	@Override 
	public int compare(Entry<OpBGP, CustomBlock> e1, Entry<OpBGP, CustomBlock> e2) { 
	  int v1 = e1.getValue().getResults(); 
	  int v2 = e2.getValue().getResults(); 
	  return v1 - v2 < 0 ? 1 : -1;
	} 
  };
  
  //This allows us to sort by number of results the bgp has, which is directly proportional to the cost of computing them
  private void sort() {
	List<Entry<OpBGP, CustomBlock>> entries = new ArrayList<Entry<OpBGP, CustomBlock>>(this.stackS.entrySet());
	Collections.sort(entries, resultsComparator);
	this.stackS = new LinkedHashMap<OpBGP, CustomBlock>();
	for (Entry<OpBGP, CustomBlock> e : entries) {
	  this.stackS.put(e.getKey(), e.getValue());
	}
  }
  
  @Override
  protected boolean addToCache(OpBGP bgp, OpTable opt) {
	if (this.queryToSolution.get(bgp) == null) {  //Case 1 k doesn't belong in the cache
	  if (this.stackS.size() < this.stackLimit) { //Case 1.1 |LIRS| < l
		this.stackS.put(bgp, new CustomBlock(true, tempResults));
		sort();
		for (Entry<OpBGP, CustomBlock> e : this.stackS.entrySet()) {
		  System.out.print(e.getValue().getResults() + ", ");
		}
		System.out.println("");
	  } else if (this.stackS.get(bgp) == null) {  //Case 1.2 k doesn't belong to the stack
	    //If S is full new block enters as HIR
		this.stackS.put(bgp, new CustomBlock(false, tempResults));
		sort();
		for (Entry<OpBGP, CustomBlock> e : this.stackS.entrySet()) {
		  System.out.print(e.getValue().getResults() + ", ");
		}
		System.out.println("");
		if (listQ.size() >= queueLimit) {
		  removeFromCache();
		}
		this.listQ.put(bgp, this.stackS.get(bgp));
	  } else if (this.stackS.get(bgp) != null) {  //Case 1.3 k belongs to the stack
		CustomBlock v = this.stackS.get(bgp);
		this.stackS.remove(bgp);
		this.stackS.put(bgp, v);
		sort();
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
	  CustomBlock v = this.stackS.get(bgp);
	  this.stackS.remove(bgp);
	  this.stackS.put(bgp, v);
	  sort();
	  //Promote to a hot block?
      v.setResident(true);
	  v.setLir(true);
	  pruneStack();
	} else if (this.stackS.get(bgp) != null && this.listQ.get(bgp) != null) { //Case 2.2 k belongs to both the stack and the list
	  CustomBlock v = this.stackS.get(bgp);
	  this.stackS.remove(bgp);
	  this.stackS.put(bgp, v);
	  sort();
	  //Promote to a hot block?
	  v.setResident(true);
	  v.setLir(true);
	  OpBGP k = this.listQ.keySet().iterator().next();
	  this.listQ.remove(k);
	  cycle();
	} else if (this.stackS.get(bgp) == null && this.listQ.get(bgp) != null) { //Case 2.3 k doesn't belong to the stack and belongs to the list
	  this.stackS.put(bgp, new CustomBlock(false, tempResults));
	  sort();
	  CustomBlock v = this.listQ.get(bgp);
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
	CustomBlock s = stackS.get(LIRSKey);
	if (s != null) {
	  s.setResident(false);
	  s.setLir(false);
	}
	//We then remove the constants from the cache
	Query qu = formQuery(LIRSKey);
	removeConstants(qu);
  }
}
