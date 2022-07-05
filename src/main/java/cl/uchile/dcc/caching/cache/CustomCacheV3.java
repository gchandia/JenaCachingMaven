package cl.uchile.dcc.caching.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTable;

class CustomBlockV3 {
  private boolean lir;
  private boolean resident;
  private int results;
  private boolean hc; //high computing cost
  //A bgp is marked when it's ready for deletion, if it has been checked once and was hc
  //It gets one chance to be promoted while marked, if it doesn't it gets deleted when marked even if it has hc
  //Remember to dismark bgp when promoting!
  private boolean marked;
  
  CustomBlockV3(boolean lir, int results) {
    this.lir = lir;
    this.resident = true;
    this.results = results;
    this.hc = false; //assume everything has low cost until list gets sorted
    this.marked = false;
  }
  
  public boolean isLir() {
	return this.lir;
  }
  
  public boolean isResident() {
	return this.resident;
  }
  
  public boolean isHighCost() {
	return this.hc;
  }
  
  public boolean isMarked() {
	return this.marked;
  }
  
  public void setLir(boolean lir) {
	this.lir = lir;
  }
  
  public void setResident(boolean resident) {
    this.resident = resident;
  }
  
  public void setHighCost(boolean hc) {
	this.hc = hc;
  }
  
  public void setMarked(boolean marked) {
	this.marked = marked;
  }
  
  public int getResults() {
	return this.results;
  }
}

public class CustomCacheV3 extends AbstractCache{
  //S contains LIR, resident and non-resident HIR blocks
  //Index 0 represent the bottom, whereas the last index represents the top
  private LinkedHashMap<OpBGP, CustomBlockV3> stackS;
  //Q contains only resident HIR blocks
  //Index 0 represents the front, whereas the last index represents the end
  private LinkedHashMap<OpBGP, CustomBlockV3> listQ;
  //We have a sorted list by number of results, we mark as low cost the results below the median, as high cost the ones above
  private LinkedHashMap<OpBGP, CustomBlockV3> sortedList;
  private int stackLimit;
  private int queueLimit;
  
  public CustomCacheV3(int itemLimit, int resultsLimit, int stackLimit, int queueLimit) {
	super(itemLimit, resultsLimit);
	this.stackS = new LinkedHashMap<OpBGP, CustomBlockV3>();
	this.listQ = new LinkedHashMap<OpBGP, CustomBlockV3>();
	this.sortedList = new LinkedHashMap<OpBGP, CustomBlockV3>();
	this.stackLimit = stackLimit;
	this.queueLimit = queueLimit;
  }
  
  private void pruneStack() {
	while (!this.stackS.get(this.stackS.keySet().iterator().next()).isLir()) this.stackS.remove(this.stackS.keySet().iterator().next());
  }
  
  private void cycle() {
	if (listQ.size() >= queueLimit) {
	  removeFromCache();
	}
	Entry<OpBGP, CustomBlockV3> y = this.stackS.entrySet().iterator().next();
	this.stackS.remove(y.getKey());
	pruneStack();
	this.listQ.put(y.getKey(), y.getValue());
	//Since it moves to q it has to be HIR
	y.getValue().setLir(false);
  }
  
  Comparator<Entry<OpBGP, CustomBlockV3>> resultsComparator = new Comparator<Entry<OpBGP,CustomBlockV3>>() {
	@Override
	public int compare(Entry<OpBGP, CustomBlockV3> e1, Entry<OpBGP, CustomBlockV3> e2) { 
	  int v1 = e1.getValue().getResults(); 
	  int v2 = e2.getValue().getResults(); 
	  return v1 - v2 < 0 ? 1 : -1;
	}
  };
  
  private void sort() {
	List<Entry<OpBGP, CustomBlockV3>> entries = new ArrayList<Entry<OpBGP, CustomBlockV3>>(this.sortedList.entrySet());
	Collections.sort(entries, resultsComparator);
	this.sortedList = new LinkedHashMap<OpBGP, CustomBlockV3>();
	for (Entry<OpBGP, CustomBlockV3> e : entries) {
	  this.sortedList.put(e.getKey(), e.getValue());
	}
  }
  
  private void markCosts() {
	List<Entry<OpBGP, CustomBlockV3>> entries = new ArrayList<Entry<OpBGP, CustomBlockV3>>(this.sortedList.entrySet());
	int size = this.sortedList.size();
	int h = size / 2;
	int median = 0;
	if (size % 2 == 0) median = (entries.get(h - 1).getValue().getResults() + entries.get(h).getValue().getResults()) / 2;
	else median = entries.get(h).getValue().getResults();
	for (Entry<OpBGP, CustomBlockV3> e : this.sortedList.entrySet()) {
	  if (e.getValue().getResults() <= median) e.getValue().setHighCost(false);
	  else e.getValue().setHighCost(true);
	}
  }
  
  @Override
  protected boolean addToCache(OpBGP bgp, OpTable opt) {
	if (this.queryToSolution.get(bgp) == null) {  //Case 1 k doesn't belong in the cache
	  if (this.stackS.size() < this.stackLimit) { //Case 1.1 |LIRS| < l
		CustomBlockV3 b = new CustomBlockV3(true, tempResults);
		this.stackS.put(bgp, b);
		this.sortedList.put(bgp, b);
		sort();
		markCosts();
	  } else if (this.stackS.get(bgp) == null) {  //Case 1.2 k doesn't belong to the stack
		//If S is full new block enters as HIR
		CustomBlockV3 b = new CustomBlockV3(false, tempResults);
		this.stackS.put(bgp, b);
		this.sortedList.put(bgp, b);
		sort();
		markCosts();
		if (listQ.size() >= queueLimit) {
		  removeFromCache();
		}
		this.listQ.put(bgp, b);
	  } else if (this.stackS.get(bgp) != null) {  //Case 1.3 k belongs to the stack
		CustomBlockV3 v = this.stackS.get(bgp);
		this.stackS.remove(bgp);
		this.stackS.put(bgp, v);
		//Promote to a hot block?
		v.setResident(true);
		v.setLir(true);
		v.setMarked(false);
		cycle();
	  }
	  queryToSolution.put(bgp, opt);
	  return true;
    }
	//Case 2 k already belongs in the cache
	if (this.stackS.get(bgp) != null && this.listQ.get(bgp) == null) { //Case 2.1 k belongs to the stack and doesn't belong to the list
	  CustomBlockV3 v = this.stackS.get(bgp);
	  this.stackS.remove(bgp);
	  this.stackS.put(bgp, v);
	  //Promote to a hot block?
      v.setResident(true);
	  v.setLir(true);
	  v.setMarked(false);
	  pruneStack();
	} else if (this.stackS.get(bgp) != null && this.listQ.get(bgp) != null) { //Case 2.2 k belongs to both the stack and the list
	  CustomBlockV3 v = this.stackS.get(bgp);
	  this.stackS.remove(bgp);
	  this.stackS.put(bgp, v);
	  //Promote to a hot block?
	  v.setResident(true);
	  v.setLir(true);
	  v.setMarked(false);
	  OpBGP k = this.listQ.keySet().iterator().next();
	  this.listQ.remove(k);
	  cycle();
	} else if (this.stackS.get(bgp) == null && this.listQ.get(bgp) != null) { //Case 2.3 k doesn't belong to the stack and belongs to the list
	  CustomBlockV3 v = this.listQ.get(bgp);
	  this.stackS.put(bgp, v);
	  this.listQ.remove(bgp);
	  this.listQ.put(bgp, v);
	  v.setMarked(false);
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
	Iterator<OpBGP> it = this.listQ.keySet().iterator();
	OpBGP bgp = it.next();
	while (true) {
	  CustomBlockV3 b = this.listQ.get(bgp);
	  if (b.isMarked()) break; //We found one marked for deletion
	  if (b.isHighCost()) {
		//If block is high cost, we reinsert it in Q marked
		b.setMarked(true);
		this.listQ.remove(bgp);
		this.listQ.put(bgp, b);
	  } else break; //If it is low cost we delete it
	  if (it.hasNext()) bgp = it.next();
	  else break;
	}
	queryToSolution.remove(bgp);
	this.listQ.remove(bgp);
	//We change the status of the block X if found in the stack S
	CustomBlockV3 s = stackS.get(bgp);
	if (s != null) {
	  s.setResident(false);
	  s.setLir(false);
	} else {
	  this.sortedList.remove(bgp);
	}
	//We then remove the constants from the cache
	Query qu = formQuery(bgp);
	removeConstants(qu);
  }
}
