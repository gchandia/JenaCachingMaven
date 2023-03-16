package cl.uchile.dcc.caching.cache;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTable;

class CustomBlockV6 {
  private boolean lir;
  private boolean resident;
  private int results;
  
  CustomBlockV6(boolean lir, int results) {
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

public class CustomCacheV6 extends AbstractCache {
  //S contains LIR, resident and non-resident HIR blocks
  //Index 0 represent the bottom, whereas the last index represents the top
  private LinkedHashMap<OpBGP, CustomBlockV6> stackS;
  //Q contains only resident HIR blocks
  //Index 0 represents the front, whereas the last index represents the end
  private LinkedHashMap<OpBGP, CustomBlockV6> listQ;
  private int stackLimit;
  private int queueLimit;
  
  public CustomCacheV6(int itemLimit, int resultsLimit, int stackLimit, int queueLimit) {
	super(itemLimit, resultsLimit);
	this.stackS = new LinkedHashMap<OpBGP, CustomBlockV6>();
	this.listQ = new LinkedHashMap<OpBGP, CustomBlockV6>();
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
	Entry<OpBGP, CustomBlockV6> y = this.stackS.entrySet().iterator().next();
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
		 this.stackS.put(bgp, new CustomBlockV6(true, tempResults));
	  } else if (this.stackS.get(bgp) == null) {  //Case 1.2 k doesn't belong to the stack
		  //If S is full new block enters as HIR
		  this.stackS.put(bgp, new CustomBlockV6(false, tempResults));
		  if (listQ.size() >= queueLimit) {
			removeFromCache();
		  }
		  this.listQ.put(bgp, this.stackS.get(bgp));
	  } else if (this.stackS.get(bgp) != null) {  //Case 1.3 k belongs to the stack
		  CustomBlockV6 v = this.stackS.get(bgp);
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
		CustomBlockV6 v = this.stackS.get(bgp);
		this.stackS.remove(bgp);
		this.stackS.put(bgp, v);
		//Promote to a hot block?
		v.setResident(true);
		v.setLir(true);
		pruneStack();
	} else if (this.stackS.get(bgp) != null && this.listQ.get(bgp) != null) { //Case 2.2 k belongs to both the stack and the list
		CustomBlockV6 v = this.stackS.get(bgp);
		this.stackS.remove(bgp);
		this.stackS.put(bgp, v);
		//Promote to a hot block?
		v.setResident(true);
		v.setLir(true);
		OpBGP k = this.listQ.keySet().iterator().next();
		this.listQ.remove(k);
		cycle();
	} else if (this.stackS.get(bgp) == null && this.listQ.get(bgp) != null) { //Case 2.3 k doesn't belong to the stack and belongs to the list
		this.stackS.put(bgp, new CustomBlockV6(false, tempResults));
		CustomBlockV6 v = this.listQ.get(bgp);
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
	//We search the block with the least time approach and remove it from Q
	Iterator<OpBGP> it = this.listQ.keySet().iterator();
	OpBGP LIRSKey = it.next();
	while(it.hasNext()) {
	  OpBGP next = it.next();
	  double numOne = this.queryToTime.get(next) * 1.0;
	  //double demOne = this.listQ.get(next).getResults() * 1.0;
	  double demOne = this.bgpResults(next) * 1.0;
	  double numTwo = this.queryToTime.get(LIRSKey) * 1.0;
	  //double demTwo = this.listQ.get(LIRSKey).getResults() * 1.0;
	  double demTwo = this.bgpResults(LIRSKey) * 1.0;
	  if (numOne/demOne < numTwo/demTwo) LIRSKey = next;
	}
	queryToSolution.remove(LIRSKey);
	listQ.remove(LIRSKey);
	//We change the status of the block X if found in the stack S
	CustomBlockV6 s = stackS.get(LIRSKey);
	if (s != null) {
	  s.setResident(false);
	  s.setLir(false);
	}
	//We then remove the constants from the cache
	Query qu = formQuery(LIRSKey);
	removeConstants(qu);
  }
}
