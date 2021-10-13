package cl.uchile.dcc.caching.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.table.TableData;
import org.apache.jena.sparql.algebra.table.TableN;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;

import cl.uchile.dcc.caching.bgps.ExtractBgps;

public class SolutionCache {
	private HashMap<OpBGP, OpTable> queryToSolution;
	private Set<Node> subjects;
	private Set<Node> objects;
	private Set<Node> predicates;
	private String solution = "";
	
	public SolutionCache() {
		this.queryToSolution = new HashMap<OpBGP, OpTable>();
		this.subjects = new HashSet<Node>();
		this.objects = new HashSet<Node>();
		this.predicates = new HashSet<Node>();
	}
	
	private void addToCache(OpBGP bgp, OpTable opt) {
		if (this.queryToSolution.get(bgp) == null) {
			this.queryToSolution.put(bgp, opt);
		}
	}
	
	public String getSolution() {
		return this.solution;
	}
	
	public void formSolution(String input) {
		this.solution = input;
	}
	
	public void printConstants() {
		System.out.println(subjects);
		System.out.println(predicates);
		System.out.println(objects);
	}
	
	public boolean isInSubjects(Node s) {
		return this.subjects.contains(s);
	}
	
	public boolean isInPredicates(Node p) {
		return this.predicates.contains(p);
	}
	
	public boolean isInObjects(Node o) {
		return this.objects.contains(o);
	}
	
	public void cache(OpBGP bgp, ResultSet results) {
		System.out.println("Attempting to cache table...");
		Table table = new TableN();
		// TEST
		int i = 0;
		while(results.hasNext()) {
			table.addBinding(results.nextBinding());
			//if (i++ == 10) break;
			i++;
			// First Cache criterion: No more than 100K rows
			if (i > 100000) {
				System.out.println("Table too large to put in cache!");
				return;
			}
		}
		OpTable opt = OpTable.create(table);
		addToCache(bgp, opt);
		System.out.println("Table cached succesfully!");
		System.out.println("TABLE SIZE IS: " + i);
	}
	
	public void cacheConstants(Query q) {
		ElementWalker.walk(q.getQueryPattern(), new ElementVisitorBase() {
			public void visit(ElementPathBlock el) {
				Iterator<TriplePath> triples = el.patternElts();
				while (triples.hasNext()) {
					TriplePath t = triples.next();
					Node s = t.getSubject();
					Node p = t.getPredicate();
					Node o = t.getObject();
					if (!s.isVariable() && !s.isBlank()) subjects.add(s);
					if (!p.isVariable() && !p.isBlank()) predicates.add(p);
					if (!o.isVariable() && !o.isBlank()) objects.add(o);
				}
			}
		});
	}
	
	public boolean isInCache(ArrayList<OpBGP> input) throws Exception {
		OpBGP bgp = ExtractBgps.unifyBGPs(input);
		bgp = ExtractBgps.canonBGP(bgp);
		boolean  finale = this.queryToSolution.get(bgp) == null ? false : true;
		return finale;
	}
	
	public boolean isBgpInCache(OpBGP input) {
		boolean finale = this.queryToSolution.get(input) == null ? false : true;
		return finale;
	}
	
	public boolean isBgpInCacheV2(OpBGP input) {
		ArrayList<OpBGP> op1 = new ArrayList<OpBGP>();
		op1.add(input);
		op1 = ExtractBgps.separateBGPs(op1);
		boolean flag = false;
		for (ArrayList<OpBGP> l : this.getSeparateKeys()) {
			flag = this.areSameBgp(op1, l);
			if (flag) break;
		}
		return flag;
	}
	
	private boolean areSameBgp(ArrayList<OpBGP> op1, ArrayList<OpBGP> op2) {
		ArrayList<OpBGP> clone = op1;
		
		for (OpBGP t2 : op2) {
			if (clone.contains(t2)) clone.remove(t2);
			else return false;
		}
		
		if (clone.size() == 0) return true;
		else return false;
	}
	
	public ArrayList<Op> extractFromCache(ArrayList<OpBGP> input, ArrayList<OpBGP> canon) {
		ArrayList<Op> output = new ArrayList<Op>();
		
		for (int i = 0; i < input.size(); i++) {
			OpBGP bgp = input.get(i);
			OpBGP cbgp = canon.get(i);
			if (this.queryToSolution.get(cbgp) == null) {
				output.add(bgp); // Add non canonicalised bgp
			} else {
				output.add(bgp);
				output.add(this.queryToSolution.get(cbgp)); // Add cached solution
			}
		}
		
		return output;
	}
	
	public ArrayList<Op> retrieve(ArrayList<Op> input, ArrayList<OpBGP> ret) throws Exception {
		ArrayList<Op> output = new ArrayList<Op>();
		OpBGP bgp = ExtractBgps.unifyBGPs(ret);
		bgp = ExtractBgps.canonBGP(bgp);
		for (int i = 0; i < input.size(); i++) {
			output.add(input.get(i));
		}
		output.add(this.queryToSolution.get(bgp));
		return output;
	}
	
	public ArrayList<Op> retrieveCache(ArrayList<Op> input, OpBGP ret) {
		ArrayList<Op> output = new ArrayList<Op>();
		
		output = input;
		output.add(this.queryToSolution.get(ret));
		
		return output;
	}
	
	public ArrayList<Op> retrieveCacheV2(ArrayList<Op> input, OpBGP ret, ArrayList<OpBGP> bgpList, Map<String, String> varMap, long startLine) {
		System.out.println("Retrieving from Cache...");
		ArrayList<Op> output = new ArrayList<Op>();
		output = input;
		
		long beforeRet = System.nanoTime();
		String br = "Time before absolute retrieving from cache: " + (beforeRet - startLine);
		OpTable table = this.queryToSolution.get(ret);
		long afterRet = System.nanoTime();
		String ar = "Time after absolute retrieving from cache: " + (afterRet - startLine);
		
		Table t = table.getTable();
		ArrayList<Var> newVars = new ArrayList<Var>();
		ArrayList<Binding> newBindings = new ArrayList<Binding>();
		
		
		long beforeFirstCycle = System.nanoTime();
		String bf = "Time before first cycle: " + (beforeFirstCycle - startLine);
        for (Var oldVar: t.getVars()) {
        	String newVarS = getKey(varMap, oldVar.toString().substring(1));
        	Var newVar = Var.alloc(newVarS.substring(1));
        	newVars.add(newVar);
        }
        long afterFirstCycle = System.nanoTime();
        String af = "Time after first cycle: " + (afterFirstCycle - startLine);
        
        Iterator<Binding> it = table.getTable().rows();
        
        long beforeSecondCycle = System.nanoTime();
        String bs = "Time before second cycle: " + (beforeSecondCycle - startLine);
       /* while (it.hasNext()) {
        	Binding b = it.next();
        	Binding nb = BindingFactory.binding(newVars.get(0), b.get(t.getVars().get(0)));
        	for (int i = 1; i < newVars.size(); i++) {
        		nb = BindingFactory.binding(nb, newVars.get(i), b.get(t.getVars().get(i)));
        	}
        	//nb = BindingFactory.binding(nb, newVars.get(1), b.get(t.getVars().get(1)));
        	newBindings.add(nb);
        }*/
        while (it.hasNext()) {
        	Binding b = it.next();
        	BindingBuilder bb = BindingBuilder.create();
        	bb = bb.set(newVars.get(0), b.get(t.getVars().get(0)));
        	for (int i = 1; i < newVars.size(); i++) {
        		bb = bb.set(newVars.get(i), b.get(t.getVars().get(i)));
        	}
        	newBindings.add(bb.build());
        }
        long afterSecondCycle = System.nanoTime();
        String as = "Time after second cycle: " + (afterSecondCycle - startLine);
        
        TableData tableDataNew = new TableData(newVars, newBindings);
        OpTable newT = OpTable.create(tableDataNew);
        
        long beforeRep = System.nanoTime();
		String bre = "Time before replaceAll: " + (beforeRep - startLine);
		for (OpBGP b : bgpList) {
        	Collections.replaceAll(output, b, newT);
        }
		long afterRep = System.nanoTime();
		String are = "Time after replaceAll: " + (afterRep - startLine);
        
		formSolution(br + '\n' + ar + '\n' + bf + '\n' + af + '\n' + bs + '\n' + as + '\n' + bre + '\n' + are);
        //output.add(OpTable.create(tableDataNew));
		return output;
	}
	
	public Op retrieveCacheV3(Op algebra, ArrayList<Op> input, OpBGP ret, ArrayList<OpBGP> bgpList, Map<String, String> varMap) {
		System.out.println("Retrieving from Cache...");
		ArrayList<Op> output = new ArrayList<Op>();
		output = input;
		
		OpTable table = this.queryToSolution.get(ret);
		
		Table t = table.getTable();
		ArrayList<Var> newVars = new ArrayList<Var>();
		ArrayList<Binding> newBindings = new ArrayList<Binding>();
		
        for (Var oldVar: t.getVars()) {
        	String newVarS = getKey(varMap, oldVar.toString().substring(1));
        	Var newVar = Var.alloc(newVarS.substring(1));
        	newVars.add(newVar);
        }
        
        Iterator<Binding> it = table.getTable().rows();
        Binding nb = null;
        while (it.hasNext()) {
        	Binding b = it.next();
        	nb = BindingFactory.binding(newVars.get(0), b.get(t.getVars().get(0)));
        	for (int i = 1; i < newVars.size(); i++) {
        		nb = BindingFactory.binding(nb, newVars.get(i), b.get(t.getVars().get(i)));
        	}
        	//nb = BindingFactory.binding(nb, newVars.get(1), b.get(t.getVars().get(1)));
        	newBindings.add(nb);
        }
        
        algebra = Substitute.substitute(algebra, nb);
        
        
        TableData tableDataNew = new TableData(newVars, newBindings);
        OpTable newT = OpTable.create(tableDataNew);
        
        for (OpBGP b : bgpList) {
        	Collections.replaceAll(output, b, newT);
        }
        //output.add(OpTable.create(tableDataNew));
		return algebra;
	}
	
	public ArrayList<OpBGP> getKeys() {
		ArrayList<OpBGP> output = new ArrayList<OpBGP>();
		for (OpBGP key: this.queryToSolution.keySet()) {
			output.add(key);
		}
		return output;
	}
	
	private <K, V> K getKey(Map<K, V> map, V value) {
	    for (Entry<K, V> entry : map.entrySet()) {
	        if (entry.getValue().equals(value)) {
	            return entry.getKey();
	        }
	    }
	    return null;
	}
	
	public ArrayList<ArrayList<OpBGP>> getSeparateKeys() {
		ArrayList<ArrayList<OpBGP>> output = new ArrayList<ArrayList<OpBGP>>();
		for (OpBGP key: this.queryToSolution.keySet()) {
			ArrayList<OpBGP> op2 = new ArrayList<OpBGP>();
			op2.add(key);
			op2 = ExtractBgps.separateBGPs(op2);
			output.add(op2);
		}
		return output;
	}
	
	public void printKeys() {
		for (OpBGP key : this.queryToSolution.keySet()) {
			System.out.println(key);
		}
	}
}