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

public class CustomCacheV2 extends AbstractCache{
  private LinkedHashMap<OpBGP, Integer> mapResults;
  private LinkedHashMap<OpBGP, Integer> LFUHits;
  private ArrayList<OpBGP> sortedBgps;
  
  public CustomCacheV2(int itemLimit, int resultsLimit) {
	super(itemLimit, resultsLimit);
	this.mapResults = new LinkedHashMap<OpBGP, Integer>();
	this.LFUHits = new LinkedHashMap<OpBGP, Integer>();
	this.sortedBgps = new ArrayList<OpBGP>();
  }
  
  Comparator<Entry<OpBGP, Integer>> resultsComparator = new Comparator<Entry<OpBGP,Integer>>() {
	@Override
	public int compare(Entry<OpBGP, Integer> e1, Entry<OpBGP, Integer> e2) {
	  int v1 = e1.getValue();
	  int v2 = e2.getValue();
	  return v1 - v2 > 0 ? 1 : -1;
	} 
  };
  
  Comparator<Entry<Integer, List<Entry<OpBGP, Integer>>>> hitsComparator = new Comparator<Entry<Integer, List<Entry<OpBGP, Integer>>>>() {
	@Override
	public int compare(Entry<Integer, List<Entry<OpBGP, Integer>>> e1, Entry<Integer, List<Entry<OpBGP, Integer>>> e2) {
	  int v1 = e1.getKey();
	  int v2 = e2.getKey();
	  return v1 - v2 > 0 ? 1 : -1;
	}
  };
  
  private void sort() {
	LinkedHashMap<Integer, LinkedHashMap<OpBGP, Integer>> sortedByHits = new LinkedHashMap<Integer, LinkedHashMap<OpBGP, Integer>>();
	LinkedHashMap<Integer, List<Entry<OpBGP, Integer>>> list = new LinkedHashMap<Integer, List<Entry<OpBGP, Integer>>>();
	for (OpBGP bgp : this.LFUHits.keySet()) {
	  int key = this.LFUHits.get(bgp);
	  if (sortedByHits.get(key) == null) {
		sortedByHits.put(key, new LinkedHashMap<OpBGP, Integer>());
	  }
	  sortedByHits.get(key).put(bgp, this.mapResults.get(bgp));
	}
	for (Integer i : sortedByHits.keySet()) {
      List<Entry<OpBGP, Integer>> resultEntries = new ArrayList<Entry<OpBGP, Integer>>(sortedByHits.get(i).entrySet());
      Collections.sort(resultEntries, resultsComparator);
      list.put(i, resultEntries);
	}
	List<Entry<Integer, List<Entry<OpBGP, Integer>>>> hitEntries = new ArrayList<Entry<Integer, List<Entry<OpBGP, Integer>>>>(list.entrySet());
	Collections.sort(hitEntries, hitsComparator);
	this.sortedBgps = new ArrayList<OpBGP>();
	for (Entry<Integer, List<Entry<OpBGP, Integer>>> e : hitEntries) {
	  for (Entry<OpBGP, Integer> e2 : e.getValue()) {
		this.sortedBgps.add(e2.getKey());
	  }
	}
  }
  
  @Override
  protected boolean addToCache(OpBGP bgp, OpTable opt) {
	if (this.queryToSolution.get(bgp) == null) {
	  this.queryToSolution.put(bgp, opt);
	  this.mapResults.put(bgp, this.tempResults);
	  this.LFUHits.put(bgp, 1);
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
	return super.retrieveCache(input, ret, bgpList, varMap, startLine);
  }
  
  @Override
  protected void removeFromCache() {
    OpBGP remove = this.sortedBgps.get(0);
    queryToSolution.remove(remove);
	this.LFUHits.remove(remove);
	this.mapResults.remove(remove);
	Query qu = formQuery(remove);
	removeConstants(qu);
  }
}
