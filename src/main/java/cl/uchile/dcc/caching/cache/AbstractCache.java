package cl.uchile.dcc.caching.cache;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.table.TableData;
import org.apache.jena.sparql.algebra.table.TableN;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.syntax.ElementGroup;
import cl.uchile.dcc.caching.bgps.ExtractBgps;

public abstract class AbstractCache implements Cache {
	protected HashMap<OpBGP, OpTable> queryToSolution;
	protected HashMap<OpBGP, Integer> queryToTime;
	private int itemLimit;
	protected int resultsLimit;
	protected Map<Node, Set<String>> mapSubjects;
    protected Map<Node, Set<String>> mapPredicates;
	protected Map<Node, Set<String>> mapObjects;
	private String solution;
	//Temporary amount of results
	protected int tempResults = 0;
	private int cacheHits = 0;
	private int retrievalHits = 0;
	
	public AbstractCache(int itemLimit, int resultsLimit) {
		this.queryToSolution = new HashMap<OpBGP, OpTable>();
		this.queryToTime = new HashMap<OpBGP, Integer>();
		this.itemLimit = itemLimit;
		this.resultsLimit = resultsLimit;
		this.mapSubjects = new HashMap<Node, Set<String>>();
		this.mapPredicates = new HashMap<Node, Set<String>>();
		this.mapObjects = new HashMap<Node, Set<String>>();
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
	
	public int bgpResults(OpBGP bgp) {
	  OpTable table = this.queryToSolution.get(bgp);
	  return table.getTable().size();
	}
	
	protected abstract boolean addToCache(OpBGP bgp, OpTable opt);
	
	public void cacheTimes(OpBGP bgp, long times) {
	  this.queryToTime.put(bgp, Math.toIntExact(times));
	}
	
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
	
	public int getCacheHits() {
	  return this.cacheHits;
	}
	
	public int getRetrievalHits() {
	  return this.retrievalHits;
	}
	
	protected void formSolution(String input) {
	  this.solution = input;
	}
	
	public void printMapConstants() {
	  System.out.println(mapSubjects);
	  System.out.println(mapPredicates);
	  System.out.println(mapObjects);
	}
	
	public Map<Node, Set<String>> getMapSubjects() {
	  return this.mapSubjects;
	}
	
	public Map<Node, Set<String>> getMapPredicates() {
	  return this.mapPredicates;
	}
	
	public Map<Node, Set<String>> getMapObjects() {
	  return this.mapObjects;
	}
	
	public boolean isInSubjects(Node s) {
	  return this.mapSubjects.containsKey(s);
	}
	
	public boolean isInPredicates(Node p) {
	  return this.mapPredicates.containsKey(p);
	}
	
	public boolean isInObjects(Node o) {
	  return this.mapObjects.containsKey(o);
	}
	
	public boolean cache(OpBGP bgp, ResultSet results) {
	  //System.out.println("Attempting to cache table with bgp " + bgp);
	  System.out.println("Attempting to cache table...");
	  
	  cleanCache();
	  
	  Table table = new TableN();
	  // TEST
	  int i = 0;
	  while(results.hasNext()) {
		//while (i < 10) {
		table.addBinding(results.nextBinding());
		//if (i++ == 10) break;
		i++;
		
		if (i > 10000) {
		  System.out.println("Table too large to put in cache!");
		  return false;
		}
	  }
	  
	  tempResults = i;
	  
	  OpTable opt = OpTable.create(table);
	  
	  if (addToCache(bgp, opt)) {
		System.out.println("Table cached succesfully!");
		this.cacheHits++;
	  } else {
		System.out.println("Table already in cache!");
	  }
	  
	  //System.out.println("TABLE SIZE IS: " + i);
	  
	  cleanCache();
      
	  return true;
	}
	
	public int getTempResults() {
	  return this.tempResults;
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
          if (mapSubjects.containsKey(s)) {
            Set<String> set = mapSubjects.get(s);
            set.add(ExtractBgps.hash256(q.toString()));
            mapSubjects.put(s, set);
          } else {
            Set<String> set = new HashSet<String>();
            set.add(ExtractBgps.hash256(q.toString()));
            mapSubjects.put(s, set);
          }
        }
        
        if (!p.isVariable() && !p.isBlank()) {
          if (mapPredicates.containsKey(p)) {
            Set<String> set = mapPredicates.get(p);
            set.add(ExtractBgps.hash256(q.toString()));
            mapPredicates.put(p, set);
          } else {
            Set<String> set = new HashSet<String>();
            set.add(ExtractBgps.hash256(q.toString()));
            mapPredicates.put(p, set);
          }
        }
        
        if (!o.isVariable() && !o.isBlank()) {
          if (mapObjects.containsKey(o)) {
            Set<String> set = mapObjects.get(o);
            set.add(ExtractBgps.hash256(q.toString()));
            mapObjects.put(o, set);
          } else {
            Set<String> set = new HashSet<String>();
            set.add(ExtractBgps.hash256(q.toString()));
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
		
		if (table.getTable().size() == 0) {
		  return output;
		}
		
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
	    
		this.retrievalHits++;
		formSolution(br + '\n' + ar + '\n' + bf + '\n' + af + '\n' + bs + '\n' + as + '\n' + bre + '\n' + are);
	    
		return output;
	}
	
	public void dumpCache(String s) {
	  File output = new File(s);
	  ObjectOutputStream o = null;
	  try {
		o = new ObjectOutputStream(new FileOutputStream(output));
		Iterator<OpBGP> it = getKeys().iterator();
		while (it.hasNext()) {
		  OpBGP b = it.next();
		  Iterator<Triple> itt = b.getPattern().iterator();
		  while (itt.hasNext()) {
			o.writeObject(itt.next());
		  }
		  o.writeChar('\n');
		}
		o.flush();
		o.close();
		System.out.println("Finished dumping cache!");
	  } catch (IOException e) {
		e.printStackTrace();
	  }
	}
	
	public void loadCache(String s, Model model) {
	  File input = new File(s);
	  ObjectInputStream oi = null;
	  try {
		oi = new ObjectInputStream(new FileInputStream(input));
	  } catch (IOException e) {
		e.printStackTrace();
	  }
	  Triple t = null;
	  try {
		t = (Triple) oi.readObject();
	  } catch (ClassNotFoundException e) {
		e.printStackTrace();
	  } catch (IOException e) {
		e.printStackTrace();
	  }
	  BasicPattern bp = new BasicPattern();
	  while (true) {
		try {
		  bp.add(t);
		  t = (Triple) oi.readObject();
		} catch (ClassNotFoundException | IOException e) {
		  OpBGP bb = new OpBGP(bp);
		  Query qq = new Query();
		  ElementGroup elg;
		  qq = QueryFactory.make();
		  qq.setQuerySelectType();
		  qq.setQueryResultStar(true);
		  elg = new ElementGroup();
		  for (Triple tt : bb.getPattern()) {
			elg.addTriplePattern(tt);
		  }
		  qq.setQueryPattern(elg);
		  ArrayList<OpBGP> qBgps = ExtractBgps.getBgps(Algebra.compile(qq));
		  QueryExecution qExec = QueryExecutionFactory.create(qq, model);
		  ResultSet qResults = qExec.execSelect();
		  cache(qBgps.get(0), qResults);
		  bp = new BasicPattern();
		  try {
			oi.readChar();
			t = (Triple) oi.readObject();
		  } catch (IOException ee) { 
			break; 
		  } catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		  }
		}
	  }
	  System.out.println("Loaded cache!");
	}
	
	protected abstract void removeFromCache();
	
	protected void removeConstants(Query qu) {
		for (Node n : mapSubjects.keySet()) {
		  Set<String> s = mapSubjects.get(n);
		  s.remove(ExtractBgps.hash256(qu.toString()));
		  if (s.size() == 0) {
		    mapSubjects.remove(n);
		  } else {
		    mapSubjects.put(n, s);
		  }
		}
		
	    for (Node n : mapPredicates.keySet()) {
		  Set<String> s = mapPredicates.get(n);
		  s.remove(ExtractBgps.hash256(qu.toString()));
		  if (s.size() == 0) {
		    mapPredicates.remove(n);
		  } else {
		    mapPredicates.put(n, s);
		  }
	    }
		      
		for (Node n : mapObjects.keySet()) {
		  Set<String> s = mapObjects.get(n);
		  s.remove(ExtractBgps.hash256(qu.toString()));
		  if (s.size() == 0) {
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
	
	public int getConstantAmount() {
	  int total = 0;
	  for (Node n : this.mapSubjects.keySet()) {
	    total += this.mapSubjects.get(n).size();
	  }
	  for (Node n : this.mapPredicates.keySet()) {
	    total += this.mapPredicates.get(n).size();
	  }
	  for (Node n : this.mapObjects.keySet()) {
	    total += this.mapObjects.get(n).size();
	  }
	  return total;
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