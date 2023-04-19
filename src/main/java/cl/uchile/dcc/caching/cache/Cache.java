package cl.uchile.dcc.caching.cache;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;

public interface Cache {
	public int cacheSize();
	public int resultsSize();
	public String getSolution();
	public boolean isInSubjects(Node s);
	public boolean isInPredicates(Node p);
	public boolean isInObjects(Node o);
	public int getCacheHits();
	public int getRetrievalHits();
	public void cacheTimes(OpBGP bgp, long times);
	public boolean cache(OpBGP bgp, 
						 ResultSet results);
	public void cacheConstants(Query q);
	public ArrayList<Op> retrieveCache(ArrayList<Op> input, 
            						   OpBGP retrieve, 
            						   ArrayList<OpBGP> bgpList, 
            						   Map<String, String> varMap, 
            						   long startLine);
	public void dumpCache(String s);
	public void loadCache(String s, Model model);
	public boolean isBgpInCache(OpBGP input);
	//For testing purposes only
	public LinkedHashMap<OpBGP, Integer> getLinkedMap();
	public int getConstantAmount();
	public int getTempResults();
}
