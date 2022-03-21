package cl.uchile.dcc.caching.cache;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTable;

public class SLRUCache extends AbstractCache {
  private LinkedHashMap<OpBGP, Integer> probationarySegment;
  private LinkedHashMap<OpBGP, Integer> protectedSegment;
  private int SLRUHit;
  private int protLimit;

  public SLRUCache(int itemLimit, int resultsLimit, int protLimit) {
	super(itemLimit, resultsLimit);
	this.probationarySegment = new LinkedHashMap<OpBGP, Integer>();
	this.protectedSegment = new LinkedHashMap<OpBGP, Integer>();
	this.SLRUHit = 0;
	this.protLimit = protLimit;
  }
  
  @Override
  protected void addToCache(OpBGP bgp, OpTable opt) {
	if (this.queryToSolution.get(bgp) == null) {
	 this.queryToSolution.put(bgp, opt);
	 this.probationarySegment.put(bgp, this.SLRUHit++);
	}
  }
  
  @Override
  public ArrayList<Op> retrieveCache(ArrayList<Op> input, 
          OpBGP ret, 
          ArrayList<OpBGP> bgpList, 
          Map<String, String> varMap, 
          long startLine) {
	probationarySegment.remove(ret);
	protectedSegment.remove(ret);
	protectedSegment.put(ret, this.SLRUHit++);
	if (protectedSegment.size() > this.protLimit) {
	  OpBGP LRUProtKey = protectedSegment.keySet().iterator().next();
	  int value = protectedSegment.remove(LRUProtKey);
	  probationarySegment.put(LRUProtKey, value);
	}
	return super.retrieveCache(input, ret, bgpList, varMap, startLine);
  }
  
  @Override
  protected void removeFromCache() {
	OpBGP SLRUKey = probationarySegment.keySet().iterator().next();
	queryToSolution.remove(SLRUKey);
	probationarySegment.remove(SLRUKey);
	
	Query qu = formQuery(SLRUKey);
    
    removeConstants(qu);
  }
}
