package cl.uchile.dcc.caching.cache;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTable;

public class LRUCache extends AbstractCache {
  private LinkedHashMap<OpBGP, Integer> LRUHits;
  private int LRUHit;
  
  public LRUCache(int itemLimit, int resultsLimit) {
	super(itemLimit, resultsLimit);
	this.LRUHits = new LinkedHashMap<OpBGP, Integer>();
	this.LRUHit = 0;
  }
  
  @Override
  protected void addToCache(OpBGP bgp, OpTable opt) {
	if (this.queryToSolution.get(bgp) == null) {
	  this.queryToSolution.put(bgp, opt);
	  this.LRUHits.put(bgp, this.LRUHit++);
	}
  }
  
  @Override
  public ArrayList<Op> retrieveCache(ArrayList<Op> input, 
	 							   	 OpBGP ret, 
									 ArrayList<OpBGP> bgpList,
									 Map<String, String> varMap, 
									 long startLine) {
	this.LRUHits.remove(ret);
	this.LRUHits.put(ret, this.LRUHit++);
	return super.retrieveCache(input, ret, bgpList, varMap, startLine);
  }
  
  @Override
  protected void removeFromCache() {
	OpBGP LRUKey = this.LRUHits.keySet().iterator().next();
	queryToSolution.remove(LRUKey);
	this.LRUHits.remove(LRUKey);
	
	Query qu = formQuery(LRUKey);
    
    removeConstants(qu);
  }
}
