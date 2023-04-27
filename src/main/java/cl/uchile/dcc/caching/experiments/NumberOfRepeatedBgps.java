package cl.uchile.dcc.caching.experiments;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.syntax.ElementGroup;

import cl.uchile.dcc.blabel.label.GraphColouring.HashCollisionException;
import cl.uchile.dcc.caching.bgps.ExtractBgps;
import cl.uchile.dcc.caching.common_joins.Joins;
import cl.uchile.dcc.caching.common_joins.Parser;
import cl.uchile.dcc.qcan.main.SingleQuery;

public class NumberOfRepeatedBgps {
  private static ArrayList<OpBGP> checkedBgpSubQueries;
  private static ArrayList<OpBGP> cachedBgpSubQueries;
  private static ArrayList<OpBGP> myBgpSubQueries;
  private static int queryNumber = 1;
  private static int attemptedToCache = 0;
  private static String qu = "";
  
  public NumberOfRepeatedBgps() {
	    checkedBgpSubQueries = new ArrayList<OpBGP>();
	    myBgpSubQueries = new ArrayList<OpBGP>();
	    cachedBgpSubQueries = new ArrayList<OpBGP>();
	  }
  
  ArrayList<OpBGP> getSubQueries(ArrayList<OpBGP> input) {
	int n = input.size();
	ArrayList<OpBGP> output = new ArrayList<OpBGP>();
	for (int x = 0; x < n; x++) {
	  int y = input.get(x).getPattern().size();
	  BasicPattern bp = new BasicPattern();
	  for (int i = 0; i < y; i++) {
		bp.add(input.get(x).getPattern().get(i));
		BasicPattern bpCopy = new BasicPattern(bp);
		output.add(new OpBGP(bpCopy));
	  }
	}
	return output;
	}
	  
  Comparator<OpBGP> subQueryComparator = new Comparator<OpBGP>() {
	public int compare(OpBGP o1, OpBGP o2) {
	  return Integer.compare(o2.getPattern().size(), o1.getPattern().size());
	}
  };
  
  private static boolean isConnectedBgp(OpBGP input) {
	// For each bgp, check if it shares a var with any other bgp
	boolean flag = true;
	if (input.getPattern().size() == 1) return true;
	for (Triple checkBgp : input.getPattern()) {
	  boolean subflag = false;
	  for (Triple checkedBgp : input.getPattern()) {
	    if (checkBgp == checkedBgp) continue;
	    if (Joins.shareVar(checkBgp, checkedBgp)) {
	      subflag = true;
	    }
	  }
	  if (!subflag) {
	    flag = false;
	    break;
	  }
	}
	return flag;
  }
  
  ArrayList<OpBGP> removeDisconnectedBgps(ArrayList<OpBGP> input) {
	ArrayList<OpBGP> output = new ArrayList<OpBGP>();
	for (OpBGP bgp : input) {
	  if (isConnectedBgp(bgp)) {
	    output.add(bgp);
	  }
	}
	return output;
  }
  
  ArrayList<OpBGP> canonicaliseBgpList(ArrayList<OpBGP> input) throws InterruptedException, HashCollisionException {
	ArrayList<OpBGP> output = new ArrayList<OpBGP>();
	
	for (OpBGP bgp : input) {
	  Query q = new Query();
	  SingleQuery sq;
	  ElementGroup elg;
	  
	  q = QueryFactory.make();
	  q.setQuerySelectType();
	  q.setQueryResultStar(true);
	  elg = new ElementGroup();
	  Iterator<Triple> it = bgp.getPattern().iterator();
	  
	  while (it.hasNext()) {
	    elg.addTriplePattern(it.next());
	  }
	  q.setQueryPattern(elg);
	  
	  sq = new SingleQuery(q.toString(), true, true, false, true);
	  q = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
	  ArrayList<OpBGP> newbgp = ExtractBgps.getBgps(Algebra.compile(q));
	  output.addAll(newbgp);
	}
	
	return output;
  }
  
  private static boolean areTriplesEquivalent(Triple a, Triple b) {
	boolean flagVarSA = a.getSubject().isVariable();
	boolean flagVarPA = a.getPredicate().isVariable();
	boolean flagVarOA = a.getObject().isVariable();
	
	boolean flagVarSB = b.getSubject().isVariable();
	boolean flagVarPB = b.getPredicate().isVariable();
	boolean flagVarOB = b.getObject().isVariable();
	
	boolean sAsB = (flagVarSA && flagVarSB) || a.getSubject().equals(b.getSubject());
	boolean pApB = (flagVarPA && flagVarPB) || a.getPredicate().equals(b.getPredicate());
	boolean oAoB = (flagVarOA && flagVarOB) || a.getObject().equals(b.getObject());
	
	return (sAsB && pApB && oAoB);
  }
  
  private static boolean isBgpSubQuery(OpBGP one, OpBGP two) {
	boolean flag = false;
	    
	ArrayList<OpBGP> bgpOne = new ArrayList<OpBGP>();
	for (Triple t : one.getPattern()) {
	  BasicPattern bp = new BasicPattern();
	  bp.add(t);
	  OpBGP b = new OpBGP(bp);
	  bgpOne.add(b);
	}
	    
	ArrayList<OpBGP> bgpTwo = new ArrayList<OpBGP>();
	for (Triple t : two.getPattern()) {
	  BasicPattern bp = new BasicPattern();
	  bp.add(t);
	  OpBGP b = new OpBGP(bp);
	  bgpTwo.add(b);
	}
	    
	for (OpBGP bgp1 : bgpOne) {
	  boolean subflag = false;
	  for (Triple t1 : bgp1.getPattern()) {
	    for (OpBGP bgp2 : bgpTwo) {
	      for (Triple t2 : bgp2.getPattern()) {
	        if (areTriplesEquivalent(t1, t2)) subflag = true;
	      }
	    }
	  }
	  if (!subflag) return flag;
	}
	    
	flag = true;
	return flag;
  }
  
  private static boolean checkForBgpSubQuery(OpBGP bgp) {
	boolean flag = false;
	for (OpBGP b : cachedBgpSubQueries) {
	  if (isBgpSubQuery(bgp, b)) flag = true;
	}
	return flag;
  }
  
  static void cleanBgpSubQueries() {
	ArrayList<OpBGP> cleanMyBgpSubQueries = new ArrayList<OpBGP>();
	for (int i = 0; i < myBgpSubQueries.size(); i++) {
	  OpBGP b = myBgpSubQueries.get(i);
	  if (b != null) {
		cleanMyBgpSubQueries.add(b);
      }
	}
	myBgpSubQueries = cleanMyBgpSubQueries;
  }
  
  static void checkBgpQueue(OpBGP bgp) {
    cleanBgpSubQueries();
	
	if (checkForBgpSubQuery(bgp)) {
	  checkedBgpSubQueries.add(bgp);
	  return;
	}
	
	for (OpBGP b : checkedBgpSubQueries) {
	  if (b.equals(bgp)) return;
	}
	
	int howManyTimesSeen = countAppearances(bgp);
	
	// 0 for 1, 1 for two repeated, and so forth
	if (howManyTimesSeen >= 1) {
	  attemptedToCache++;
	  checkedBgpSubQueries.add(bgp);
	  cachedBgpSubQueries.add(bgp);
	  removeRepeatedBgp(bgp);
	  return;
	}
	
	checkedBgpSubQueries.add(bgp);
	if (myBgpSubQueries.size() < 100000) {
	  myBgpSubQueries.add(bgp);
	} else {
	  myBgpSubQueries.remove(0);
	  myBgpSubQueries.add(bgp);
	}
  }
  
  private static void removeRepeatedBgp(OpBGP bgp) {
    ArrayList<OpBGP> newSubQueries = new ArrayList<OpBGP>();
    for (OpBGP b : myBgpSubQueries) {
      if (!b.equals(bgp)) newSubQueries.add(b);
    }
    myBgpSubQueries = newSubQueries;
  }
  
  private static int countAppearances(OpBGP bgp) {
	int howManyTimes = 0;
	for (OpBGP b : myBgpSubQueries) {
	  if (b.equals(bgp)) howManyTimes++;
	}
	return howManyTimes;
  }

  void checkBgps(ArrayList<OpBGP> bgps) {
	checkedBgpSubQueries = new ArrayList<OpBGP>();
	cachedBgpSubQueries = new ArrayList<OpBGP>();
	for (OpBGP bgp : bgps) {
	  checkBgpQueue(bgp);
	}
  }
  
  public static void main(String[] args) throws IOException {
	InputStream is = new FileInputStream(new File("/home/gchandia/wikidata_logs/FilteredLogs.tsv"));
	final Scanner sc = new Scanner(is);
	
	final PrintWriter w = new PrintWriter(new FileWriter("/home/gchandia/Thesis/Buffer10K.txt"));
	
	final NumberOfRepeatedBgps rb = new NumberOfRepeatedBgps();
	
	// 2133607
	while (sc.hasNextLine()) {
	  final Runnable stuffToDo = new Thread() {
	    @Override
	    public void run() {
	      try {
	        System.out.println("READING QUERY " + queryNumber++);
	        qu = sc.nextLine();
	        Parser parser = new Parser();
	        Query q = parser.parseDbPedia(qu);
	        
	        //System.out.println(q);
	        //System.out.println(Algebra.compile(q));
	        
	        //Get bgps from optimized query q
	        ArrayList<OpBGP> bgps = ExtractBgps.getBgps(Algebra.optimize(Algebra.compile(q)));
	        
	        //Only get subqueries with highest priority TP for each bgp
	        ArrayList<OpBGP> subQueries = rb.getSubQueries(bgps);
	        //Sort subqueries from biggest to smallest
	        Collections.sort(subQueries, rb.subQueryComparator);
	        subQueries = rb.removeDisconnectedBgps(subQueries);
	        ArrayList<OpBGP> bgpsq = rb.canonicaliseBgpList(subQueries);
	        System.out.println("Number of subqueries is: " + bgpsq.size());
	        rb.checkBgps(bgpsq);
	        
	        w.println("Info for query number " + (queryNumber - 1));
	        w.println(attemptedToCache);
	        
	      } catch (Exception e) {
	    	w.println("Info for query number " + (queryNumber - 1)); 
	        e.printStackTrace(w);
	        w.println();
	      }
	    }
	  };
	  
	  final ExecutorService executor = Executors.newSingleThreadExecutor();
	  @SuppressWarnings("rawtypes")
	  final Future future = executor.submit(stuffToDo);
	  executor.shutdown();
	  
	  try {
	    future.get(15, TimeUnit.SECONDS);
	  } catch (InterruptedException ie) {}
	    catch (ExecutionException ee) {}
	    catch (TimeoutException te) {}
	  }
	sc.close();
	w.close();
  }
}
