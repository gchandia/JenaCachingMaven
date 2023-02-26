package cl.uchile.dcc.caching.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTable;

public class LFUCache extends AbstractCache {
  private LinkedHashMap<OpBGP, Integer> LFUHits;
  private List<Map.Entry<OpBGP, Integer>> sorted;
  
  public void sort() {
	Collections.sort(this.sorted, new Comparator<Map.Entry<OpBGP, Integer>>() {
	  public int compare(Map.Entry<OpBGP, Integer> a, Map.Entry<OpBGP, Integer> b){
	    return a.getValue().compareTo(b.getValue());
	  }
	});
  }
  
  public LFUCache(int itemLimit, int resultsLimit) {
	super(itemLimit, resultsLimit);
	this.LFUHits = new LinkedHashMap<OpBGP, Integer>();
  }
  
  @Override
  protected boolean addToCache(OpBGP bgp, OpTable opt) {
	if (this.queryToSolution.get(bgp) == null) {
	  this.queryToSolution.put(bgp, opt);
	  this.LFUHits.put(bgp, 1);
	  sorted = new ArrayList<Map.Entry<OpBGP, Integer>>(this.LFUHits.entrySet());
	  sort();
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
	sorted = new ArrayList<Map.Entry<OpBGP, Integer>>(this.LFUHits.entrySet());
	sort();
	return super.retrieveCache(input, ret, bgpList, varMap, startLine);
  }
  
  @Override
  protected void removeFromCache() {
	OpBGP LFUKey = this.sorted.get(0).getKey();
	queryToSolution.remove(LFUKey);
	this.LFUHits.remove(LFUKey);
	
	Query qu = formQuery(LFUKey);
	
	removeConstants(qu);
  }
  
  public LinkedHashMap<OpBGP, Integer> getLinkedMap() {
	return this.LFUHits;
  }
}
