package cl.uchile.dcc.caching.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpQuad;

import cl.uchile.dcc.caching.bgps.ExtractBgps;
import cl.uchile.dcc.caching.bgps.ManipulateBgps;
import cl.uchile.dcc.caching.cache.Cache;
import cl.uchile.dcc.caching.common_joins.Joins;

public class CacheTransformCopy extends TransformCopy {
	private Cache myCache;
	private long startLine = 0;
	private String solution = "";
	//List of triple patterns passed by ExperimentPolicyFile
	private ArrayList<OpBGP> listOfTPs = new ArrayList<OpBGP>();
	private ArrayList<OpBGP> nonFilteredBgps = new ArrayList<OpBGP>();
	
	public CacheTransformCopy(Cache cache) {
	  this.myCache = cache;
	}
	
	public CacheTransformCopy(Cache cache, long startLine) {
	  this.myCache = cache;
	  this.startLine = startLine;
	}
	
	public CacheTransformCopy(Cache cache, long startLine, ArrayList<OpBGP> listOfTPs) {
	  this.myCache = cache;
	  this.startLine = startLine;
	  this.listOfTPs = listOfTPs;
	}
	
	public String getSolution() {
		return this.solution;
	}
	
	public void formSolution(String input) {
		this.solution = input;
	}
	
	public ArrayList<OpBGP> filterBgps(ArrayList<OpBGP> input) {
		ArrayList<OpBGP> output = new ArrayList<OpBGP>();
		for (OpBGP bgp : input) {
			Node s = bgp.getPattern().get(0).getSubject();
			Node p = bgp.getPattern().get(0).getPredicate();
			Node o = bgp.getPattern().get(0).getObject();
			
			if ((s.isVariable() || s.isBlank()) && (p.isVariable() || p.isBlank()) && (o.isVariable() || o.isBlank())) {
				nonFilteredBgps.add(bgp);
				continue;
			}
			
			if (!s.isVariable() && !s.isBlank()) {
				if (!myCache.isInSubjects(s)) {
					nonFilteredBgps.add(bgp);
					continue;
				}
			}
			
			if (!p.isVariable() && !p.isBlank()) {
				if (!myCache.isInPredicates(p)) {
					nonFilteredBgps.add(bgp);
					continue;
				}
			}
			
			if (!o.isVariable() && !o.isBlank()) {
				if (!myCache.isInObjects(o)) {
					nonFilteredBgps.add(bgp);
					continue;
				}
			}
			
			output.add(bgp);
		}
		return output;
	}
	
	public Op transform(OpQuad opQuad) {
		return new OpQuad(opQuad.getQuad());
	}
	
	public Op transform(OpBGP bgp) {
		//System.out.println("Bgp is:\n" + bgp);
		//myCache.printConstants();
		
		// Get query bgps
		//ArrayList<OpBGP> bgps = ExtractBgps.getBgps(bgp);
		//bgps = ExtractBgps.separateBGPs(bgps);
		
		//Refresh non filtered bgps
		nonFilteredBgps = new ArrayList<OpBGP>();
		
		// Filter bgps that have constants in the cache
		ArrayList<OpBGP> bgps = filterBgps(this.listOfTPs);
		//System.out.println("Filtered bgps are: " + bgps);
		long fb = System.nanoTime();
		String filterBgps = "Time to filter bgps: " + (fb - startLine);
		
		// Get all query subbgps of size two and more
		ArrayList<ArrayList<OpBGP>> subBgps = Joins.getSubBGPs(bgps);
		long gs = System.nanoTime();
		String getSubBgps = "Time to get sub bgps: " + (gs - startLine);
		
		// We're sorting all subbgps from biggest to smallest here
		ArrayList<ArrayList<OpBGP>> sortedSubBgps = new ArrayList<ArrayList<OpBGP>>();
		
		// Add subbgps of size two and more
		for (int i = subBgps.size() - 1; i >= 0; i--) {
			sortedSubBgps.add(subBgps.get(i));
		}
		
		// Extract size one subBgps
		ArrayList<OpBGP> sizeOneBgps = bgps;
		
		// Add size one subBgps to our sorted array
		for (int i = sizeOneBgps.size() - 1; i >= 0; i--) {
			ArrayList<OpBGP> sl = new ArrayList<OpBGP>();
			sl.add(sizeOneBgps.get(i));
			sortedSubBgps.add(sl);
		}
		
		long ac = System.nanoTime();
		String afterCycles = "Time for both list cycles: " + (ac - startLine);
		
		// We add each Op instance in bgps to the array we'll work on with the cache
		ArrayList<Op> cachedBgps = new ArrayList<Op>();
		cachedBgps.addAll(sizeOneBgps);
		
		long beforeCache = System.nanoTime();
		String bc = "Time before registering cache is: " + (beforeCache - startLine);
		String ec = "DIDN'T ENTER";
		String br = "DIDN'T RETRIEVE";
		String ar = "DIDN'T RETRIEVE";
		String sol = "DIDN'T RETRIEVE";
		long canons = 0;
		
		for (ArrayList<OpBGP> bgpList : sortedSubBgps) {
			ArrayList<OpBGP> canonbgpList = ExtractBgps.separateBGPs(bgpList);
			//ArrayList<ArrayList<OpBGP>> perms = ArrayPermutations.generatePerm(canonbgpList);
			
			//for (ArrayList<OpBGP> cb : perms) {
			bgp = ExtractBgps.unifyBGPs(canonbgpList);
			Map<String, String> vars = new HashMap<String, String>();
			try {
				long beforeCanon = System.nanoTime();
				bgp = ExtractBgps.canonBGP(bgp);
				long afterCanon = System.nanoTime();
				canons += (afterCanon - beforeCanon);
				vars = ExtractBgps.getVarMap();
			} catch (Exception e) {}
			
			if (myCache.isBgpInCache(bgp) && ManipulateBgps.checkIfInBgp(cachedBgps, bgpList)) {
				ec = "ENTERED CACHE";
				long whenCache = System.nanoTime();
				br = "Time before retrieving from cache: " + (whenCache - startLine);
				cachedBgps = myCache.retrieveCache(cachedBgps, bgp, bgpList, vars, startLine);
				sol = myCache.getSolution();
				long afterCache = System.nanoTime();
				ar = "Time after retrieving from cache: " + (afterCache - startLine);
			}
		}
		
		String sCanon = "Time to canonicalize all subqueries: " + canons;
		
		// Join Bgps
		Op join = null;
			
		if (cachedBgps.size() == 1) {
			join = cachedBgps.get(0);
		} else if (cachedBgps.size() > 1){
			join = OpJoin.create(cachedBgps.get(0), cachedBgps.get(1));
		}
		
		for (int i = 2; i < cachedBgps.size(); i++) {
			join = OpJoin.create(join, cachedBgps.get(i));
		}
		
		// Add the bgps that haven't been filtered
		for (int i = 0; i < this.nonFilteredBgps.size(); i++) {
			join = OpJoin.create(join, this.nonFilteredBgps.get(i));
		}
		
		Op opjoin = Algebra.optimize(join);
		
		formSolution(filterBgps + '\n' + getSubBgps + '\n' + afterCycles + '\n' + bc + '\n' + sCanon + '\n' + 
					 ec + '\n' + br + '\n' + sol + '\n' + ar);
		
		return opjoin;
	}
	
	/*public Op transform (OpPath op) {
		TriplePath path = ((OpPath)op).getTriplePath();
		BasicPattern bp = new BasicPattern();
		Triple nt = Triple.create(path.getSubject(), NodeFactory.createURI(path.getPath().toString()), path.getObject());
		bp.add(nt);
		
		
		return transform(new OpBGP(bp));
	}*/
}