package cl.uchile.dcc.caching.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.table.TableData;
import org.apache.jena.sparql.algebra.table.TableN;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.syntax.ElementGroup;
import cl.uchile.dcc.caching.bgps.ExtractBgps;

public abstract class AbstractCache implements Cache {
	protected HashMap<OpBGP, OpTable> queryToSolution;
	private int itemLimit;
	private int resultsLimit;
	protected Set<Node> subjects;
    protected Set<Node> predicates;
	protected Set<Node> objects;
	protected Map<Node, Set<Query>> mapSubjects;
    protected Map<Node, Set<Query>> mapPredicates;
	protected Map<Node, Set<Query>> mapObjects;
	private String solution;
	
	public AbstractCache(int itemLimit, int resultsLimit) {
		this.queryToSolution = new HashMap<OpBGP, OpTable>();
		this.itemLimit = itemLimit;
		this.resultsLimit = resultsLimit;
		this.subjects = new HashSet<Node>();
        this.predicates = new HashSet<Node>();
		this.objects = new HashSet<Node>();
		this.mapSubjects = new HashMap<Node, Set<Query>>();
		this.mapPredicates = new HashMap<Node, Set<Query>>();
		this.mapObjects = new HashMap<Node, Set<Query>>();
		this.solution = "";
	}
	
	public int cacheSize() {
	  return queryToSolution.size();
	}
	
	public int resultsSize() {
	  int results = 0;
	  for (OpBGP bgp : this.queryToSolution.keySet()) {
	    OpTable table = this.queryToSolution.get(bgp);
	    Table t = table.getTable();
	    results += t.size();
	  }
	  return results;
	}
	
	protected abstract void addToCache(OpBGP bgp, OpTable opt);
	
	protected void cleanCache() {
	  while (this.queryToSolution.size() > this.itemLimit) {
	    this.removeFromCache();
	  }
	  while (resultsSize() > this.resultsLimit) {
	    this.removeFromCache();
	  }
	}
	
	public String getSolution() {
		return this.solution;
	}
	
	protected void formSolution(String input) {
	  this.solution = input;
	}
	
	public void printConstants() {
	  System.out.println(subjects);
	  System.out.println(predicates);
	  System.out.println(objects);
	}
	
	public void printMapConstants() {
	  System.out.println(mapSubjects);
	  System.out.println(mapPredicates);
	  System.out.println(mapObjects);
	}
	
	public Set<Node> getSubjects() {
	  return this.subjects;
	}
	
	public Set<Node> getPredicates() {
	  return this.predicates;
	}
	
	public Set<Node> getObjects() {
	  return this.objects;
	}
	
	public Map<Node, Set<Query>> getMapSubjects() {
	  return this.mapSubjects;
	}
	
	public Map<Node, Set<Query>> getMapPredicates() {
	  return this.mapPredicates;
	}
	
	public Map<Node, Set<Query>> getMapObjects() {
	  return this.mapObjects;
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
	
	public boolean cache(OpBGP bgp, ResultSet results) {
	  //System.out.println("Attempting to cache table with bgp " + bgp);
	  System.out.println("Attempting to cache table...");
	  
	  Table table = new TableN();
	  // TEST
	  int i = 0;
	  while(results.hasNext()) {
		//while (i < 10) {
		table.addBinding(results.nextBinding());
		//if (i++ == 10) break;
		i++;
		// First Cache criterion: No more than 100K rows
		//i2 = maximum number of results
		if (i > 100000) {
			System.out.println("Table too large to put in cache!");
			return false;
		}
	  }
	  
	  if (table.size() == 0) {
		System.out.println("Table is empty, not caching.");
	  	return false;
	  }
	  
	  OpTable opt = OpTable.create(table);
	  addToCache(bgp, opt);
	  System.out.println("Table cached succesfully!");
	  System.out.println("TABLE SIZE IS: " + i);
	  
	  cleanCache();
      
	  return true;
	}
	
	public void cacheConstants(Query q) {
	  ArrayList<OpBGP> qBgps = ExtractBgps.getBgps(Algebra.compile(q));
	  Query qu = QueryFactory.make();
      qu.setQuerySelectType();
      qu.setQueryResultStar(true);
      ElementGroup elg = new ElementGroup();
      for (int i = 0; i < qBgps.size(); i++) {
        for (int j = 0; j < qBgps.get(i).getPattern().size(); j++) {
          elg.addTriplePattern(qBgps.get(i).getPattern().get(j));
        }
      }
      qu.setQueryPattern(elg);
      q = qu;
	  
	  ArrayList<OpBGP> bgps = ExtractBgps.getBgps(Algebra.compile(q));
	  List<Triple> triples = new ArrayList<Triple>();
	  for (OpBGP bgp : bgps) {
	    triples.addAll(bgp.getPattern().getList());
	  }
	  Iterator<Triple> it = triples.iterator();
	  while (it.hasNext()) {
	    Triple t = it.next();
        Node s = t.getSubject();
        Node p = t.getPredicate();
        Node o = t.getObject();
        
        if (!s.isVariable() && !s.isBlank()) {
          subjects.add(s);
          if (mapSubjects.containsKey(s)) {
            Set<Query> set = mapSubjects.get(s);
            set.add(q);
            mapSubjects.put(s, set);
          } else {
            Set<Query> set = new HashSet<Query>();
            set.add(q);
            mapSubjects.put(s, set);
          }
        }
        
        if (!p.isVariable() && !p.isBlank()) {
          predicates.add(p);
          if (mapPredicates.containsKey(p)) {
            Set<Query> set = mapPredicates.get(p);
            set.add(q);
            mapPredicates.put(p, set);
          } else {
            Set<Query> set = new HashSet<Query>();
            set.add(q);
            mapPredicates.put(p, set);
          }
        }
        
        if (!o.isVariable() && !o.isBlank()) {
          objects.add(o);
          if (mapObjects.containsKey(o)) {
            Set<Query> set = mapObjects.get(o);
            set.add(q);
            mapObjects.put(o, set);
          } else {
            Set<Query> set = new HashSet<Query>();
            set.add(q);
            mapObjects.put(o, set);
          }
        }
	  }
	}
	
	public boolean isBgpInCache(OpBGP input) {
		boolean finale = this.queryToSolution.get(input) == null ? false : true;
		return finale;
	}
	
	public ArrayList<Op> retrieveCache(ArrayList<Op> input, 
	                                   OpBGP ret, 
	                                   ArrayList<OpBGP> bgpList, 
	                                   Map<String, String> varMap, 
	                                   long startLine) {
		System.out.println("Retrieving from Cache...");
		
		if (input == null) { return null; }
		
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
	      Var newVar = Var.alloc(newVarS.substring(0));
	      newVars.add(newVar);
	    }
	    
	    long afterFirstCycle = System.nanoTime();
	    String af = "Time after first cycle: " + (afterFirstCycle - startLine);
	      
	    Iterator<Binding> it = table.getTable().rows();
	      
	    long beforeSecondCycle = System.nanoTime();
	    String bs = "Time before second cycle: " + (beforeSecondCycle - startLine);
	     
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
	    
		return output;
	}
	
	protected abstract void removeFromCache();
	
	protected void removeConstants(Query qu) {
		for (Node n : mapSubjects.keySet()) {
		  Set<Query> s = mapSubjects.get(n);
		  s.remove(qu);
		  if (s.size() == 0) {
		    subjects.remove(n);
		    mapSubjects.remove(n);
		  } else {
		    mapSubjects.put(n, s);
		  }
		}
		      
	    for (Node n : mapPredicates.keySet()) {
		  Set<Query> s = mapPredicates.get(n);
		  s.remove(qu);
		  if (s.size() == 0) {
		    predicates.remove(n);
		    mapPredicates.remove(n);
		  } else {
		    mapPredicates.put(n, s);
		  }
	    }
		      
		for (Node n : mapObjects.keySet()) {
		  Set<Query> s = mapObjects.get(n);
		  s.remove(qu);
		  if (s.size() == 0) {
		    objects.remove(n);
		    mapObjects.remove(n);
		  } else {
		    mapObjects.put(n, s);
		  }
		}
	}
	
	protected Query formQuery(OpBGP key) {
	  Query qu = QueryFactory.make();
	  qu.setQuerySelectType();
	  qu.setQueryResultStar(true);
	  ElementGroup elg = new ElementGroup();
	  
	  for (int j = 0; j < key.getPattern().size(); j++) {
	    elg.addTriplePattern(key.getPattern().get(j));
	  }
	  
	  qu.setQueryPattern(elg);
	  return qu;
	}
	
	public ArrayList<OpBGP> getKeys() {
		ArrayList<OpBGP> output = new ArrayList<OpBGP>();
		for (OpBGP key: this.queryToSolution.keySet()) {
			output.add(key);
		}
		return output;
	}
	
	protected <K, V> K getKey(Map<K, V> map, V value) {
	    for (Entry<K, V> entry : map.entrySet()) {
	        if (entry.getValue().equals(value)) {
	            return entry.getKey();
	        }
	    }
	    return null;
	}
	
	public LinkedHashMap<OpBGP, Integer> getLinkedMap() {
	  return null;
	}
}