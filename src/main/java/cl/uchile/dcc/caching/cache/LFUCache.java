package cl.uchile.dcc.caching.cache;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTable;

public class LFUCache extends AbstractCache {
  private LinkedHashMap<OpBGP, Integer> LFUHits;
  
  public LFUCache(int itemLimit, int resultsLimit) {
	super(itemLimit, resultsLimit);
	this.LFUHits = new LinkedHashMap<OpBGP, Integer>();
  }
  
  @Override
  protected boolean addToCache(OpBGP bgp, OpTable opt) {
	if (this.queryToSolution.get(bgp) == null) {
	  this.queryToSolution.put(bgp, opt);
	  this.LFUHits.put(bgp, 1);
	  return true;
	}
	return false;
  }
  
  @Override
  public ArrayList<Op> retrieveCache(ArrayList<Op> input, 
	 							   	 OpBGP ret, 
									 ArrayList<OpBGP> bgpList,
									 Map<String, String> varMap, 
									 long startLine) {
	this.LFUHits.put(ret, this.LFUHits.get(ret) + 1);
	return super.retrieveCache(input, ret, bgpList, varMap, startLine);
  }
  
  private OpBGP searchLFUKey() {
	OpBGP LFUKey = this.LFUHits.entrySet().iterator().next().getKey();
	int minimum = this.LFUHits.entrySet().iterator().next().getValue();
	
	for (OpBGP key : this.LFUHits.keySet()) {
	  int value = this.LFUHits.get(key);
	  if (value < minimum) {
	    minimum = value;
	    LFUKey = key;
	  }
	}
	return LFUKey;
  }
  
  @Override
  protected void removeFromCache() {
	OpBGP LFUKey = searchLFUKey();
	queryToSolution.remove(LFUKey);
	this.LFUHits.remove(LFUKey);
	
	Query qu = formQuery(LFUKey);
	
	removeConstants(qu);
  }
  
  public LinkedHashMap<OpBGP, Integer> getLinkedMap() {
	return this.LFUHits;
  }
}
