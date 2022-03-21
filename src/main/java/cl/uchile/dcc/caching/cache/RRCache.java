package cl.uchile.dcc.caching.cache;

import java.util.Iterator;
import java.util.Random;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTable;

public class RRCache extends AbstractCache {
	
	public RRCache(int sizeLimit, int resultsLimit) {
	  super(sizeLimit, resultsLimit);
	}

	@Override
	protected void addToCache(OpBGP bgp, OpTable opt) {
	  if (this.queryToSolution.get(bgp) == null) {
		this.queryToSolution.put(bgp, opt);
	  }
	}
	
	private OpBGP searchRandomKey() {
	  Random r = new Random();
	  int removeIndex = r.nextInt(queryToSolution.size() + 1);
	  OpBGP remove = queryToSolution.keySet().iterator().next();
	  Iterator<OpBGP> it = queryToSolution.keySet().iterator();
	  for (int i = 0; i < removeIndex; i++) { remove = it.next(); }
	  return remove;
	}
	
	@Override
	protected void removeFromCache() {
	  OpBGP rrKey = searchRandomKey();
	  queryToSolution.remove(rrKey);
	  
	  Query qu = formQuery(rrKey);
	  
	  removeConstants(qu);
	}
}
