package cl.uchile.dcc.caching.cachingExperiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.tdb.TDBFactory;

import cl.uchile.dcc.blabel.label.GraphColouring.HashCollisionException;
import cl.uchile.dcc.caching.bgps.ExtractBgps;
import cl.uchile.dcc.caching.cache.Cache;
import cl.uchile.dcc.caching.common_joins.Joins;
import cl.uchile.dcc.caching.common_joins.Parser;
import cl.uchile.dcc.caching.transform.CacheTransformCopy;
import cl.uchile.dcc.qcan.main.SingleQuery;

public class LogReaderThreadPool {
  private static Cache myCache;
  private static ArrayList<OpBGP> checkedBgpSubQueries;
  private static ArrayList<OpBGP> myBgpSubQueries;
  private static ArrayList<OpBGP> cachedBgpSubQueries;
  private static int queryNumber;
  private static String qu = "";
  private static String myModel = "/home/gchandia/WikiDB";
  private static Dataset ds = TDBFactory.createDataset(myModel);
  private static Model model;
  private static final int NUM_THREADS = 8;
  private static final ExecutorService THREAD_POOL = Executors.newFixedThreadPool(NUM_THREADS);
  private static ArrayList<Future<?>> futures;
  
  public LogReaderThreadPool(Cache myMyCache, ArrayList<OpBGP> myMyBgpSubQueries) {
	myCache = myMyCache;
	checkedBgpSubQueries = new ArrayList<OpBGP>();
	myBgpSubQueries = myMyBgpSubQueries;
	cachedBgpSubQueries = new ArrayList<OpBGP>();
	ds.begin(ReadWrite.READ);
    model = ds.getDefaultModel();
    futures = new ArrayList<Future<?>>();
  }
  
  public void setQueryNumber(int myQueryNumber) {
	queryNumber = myQueryNumber;
  }
  
  public Cache getCache() {
	return myCache;
  }
  
  public ArrayList<OpBGP> getMyBgpSubQueries() {
	return myBgpSubQueries;
  }
  
  static ArrayList<OpBGP> getSubQueriesV4(ArrayList<OpBGP> input) {
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
    
  static Comparator<OpBGP> subQueryComparator = new Comparator<OpBGP>()
  {
    public int compare(OpBGP o1, OpBGP o2)
    {
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
  
  static ArrayList<OpBGP> removeDisconnectedBgps(ArrayList<OpBGP> input) {
    ArrayList<OpBGP> output = new ArrayList<OpBGP>();
    for (OpBGP bgp : input) {
      if (isConnectedBgp(bgp)) {
	    output.add(bgp);
	  }
    }
	return output;
  }
    
  public static long getTimeApproach(Query q) {
	Op alg = Algebra.compile(q);
	alg = Algebra.optimize(alg);
	QueryIterator qit = Algebra.exec(alg, model);
	
	long beforeOneResult = System.nanoTime();
	qit.next();
	long afterOneResult = System.nanoTime();
	
	return (afterOneResult - beforeOneResult);
  }
  
  static ArrayList<OpBGP> canonicaliseBgpList(ArrayList<OpBGP> input) throws InterruptedException, HashCollisionException {
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
      output.addAll(newbgp);	    }
    
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
  
  static void cacheBgpQuery(OpBGP bgp) {
    ds.begin(ReadWrite.READ);
    ArrayList<OpBGP> bgps = new ArrayList<OpBGP>();
    for (Triple t : bgp.getPattern()) {
      BasicPattern bp = new BasicPattern();
      bp.add(t);
      OpBGP b = new OpBGP(bp);
      bgps.add(b);
    }
    
    //i1 minimum number of TPs to cache the query
    if (bgps.size() < 1) {
      return;
    }
    
    Query q = new Query();
    ElementGroup elg;
    q = QueryFactory.make();
    q.setQuerySelectType();
    q.setQueryResultStar(true);
    elg = new ElementGroup();
    Iterator<OpBGP> it = bgps.iterator();
    while (it.hasNext()) {
      OpBGP b = it.next();
      Triple t = b.getPattern().get(0);
      elg.addTriplePattern(t);
    }
    q.setQueryPattern(elg);
    
    ArrayList<OpBGP> qBgps = ExtractBgps.getBgps(Algebra.compile(q));
    QueryExecution qExec = QueryExecutionFactory.create(q, model);
    QueryExecution qExecTwo = QueryExecutionFactory.create(q, model);
    ResultSet qResults = qExec.execSelect();
    ResultSet backupResults = qExecTwo.execSelect();
    if (myCache.cache(qBgps.get(0), qResults)) {
      if (backupResults.hasNext()) myCache.cacheTimes(qBgps.get(0), getTimeApproach(q));
      else myCache.cacheTimes(qBgps.get(0), 0);
    }
    ds.end();
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
  
  private static boolean checkForBgpSubQuery(OpBGP bgp) {
	boolean flag = false;
    for (OpBGP b : cachedBgpSubQueries) {
      if (isBgpSubQuery(bgp, b)) flag = true;
    }
    return flag;
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
    
    for (OpBGP b : myBgpSubQueries) {
      //System.out.println("b EQUALS " + b);
      if (b.equals(bgp)) {
    	cacheBgpQuery(bgp);
        checkedBgpSubQueries.add(bgp);
        cachedBgpSubQueries.add(bgp);
        myBgpSubQueries.remove(b);
        return;
      }
    }
    
    checkedBgpSubQueries.add(bgp);
    //Change bgp buffer size
    if (myBgpSubQueries.size() < 10000) {
      myBgpSubQueries.add(bgp);
    } else {
      myBgpSubQueries.remove(0);
      myBgpSubQueries.add(bgp);
    }
  }
  
  static void checkBgps(ArrayList<OpBGP> bgps) {
	checkedBgpSubQueries = new ArrayList<OpBGP>();
    cachedBgpSubQueries = new ArrayList<OpBGP>();
    for (OpBGP bgp : bgps) {
      checkBgpQueue(bgp);
    }
  }
  
  private static void processQuery(Query q, PrintWriter w) {
	String output = "";
    try {
      System.out.println("READING QUERY " + queryNumber++);
      
      long startLine = System.nanoTime();
      
      long afterParse = System.nanoTime();
      String ap = "Time to parse: " + (afterParse - startLine);
      
      //Get bgps from optimized query q
      ArrayList<OpBGP> bgps = ExtractBgps.getBgps(Algebra.optimize(Algebra.compile(q)));
      
      //Separates bgps into chunks of size 1, hence containing all triple patterns
      ArrayList<OpBGP> numberOfTPs = ExtractBgps.separateBGPs(bgps);
      
      long bpfTime = System.nanoTime();
      String bpf = "Time before prefunctions: " + (bpfTime - startLine);
      
      String gsq = "";
      String brd = "";
      String ard = "";
      String cbl = "";
      String nbgps = "";
      
      // If there are 10 or less TPs in the query
      if (numberOfTPs.size() <= 10) {
        //Only get subqueries with highest priority TP for each bgp
        ArrayList<OpBGP> subQueries = getSubQueriesV4(bgps);
        long gsqTime = System.nanoTime();
        gsq = "Time to run getSubQueries: " + (gsqTime - startLine);
        //Sort subqueries from biggest to smallest
        Collections.sort(subQueries, subQueryComparator);
        long brdTime = System.nanoTime();
        brd = "Time before removing disconnected bgps: " + (brdTime - startLine);
        subQueries = removeDisconnectedBgps(subQueries);
        long ardTime = System.nanoTime();
        ard = "Time after removing disconnected bgps: " + (ardTime - startLine);
        nbgps = "Number of bgps to canonicalise: " + subQueries.size();
        ArrayList<OpBGP> bgpsq = canonicaliseBgpList(subQueries);
        long cblTime = System.nanoTime();
        cbl = "Time after canonicalising bgpList: " + (cblTime - startLine);
        //System.out.println("Number of subqueries is: " + bgpsq.size());
        checkBgps(bgpsq);
      }
      
      Op inputOp = Algebra.compile(q);
      Transform cacheTransform = new CacheTransformCopy(myCache, startLine, numberOfTPs);
      Op cachedOp = Transformer.transform(cacheTransform, inputOp);
      
      String solution = ((CacheTransformCopy) cacheTransform).getSolution();
      long beforeOptimize = System.nanoTime();
      String bo = "Time before optimizing: " + (beforeOptimize - startLine);
      
      Op opjoin = cachedOp;
      long start = System.nanoTime();
      String br = "Time before reading results: " + (start - startLine);
      
      Query qFinal = OpAsQuery.asQuery(opjoin);
      
      ds.begin(ReadWrite.READ);
      //QueryExecution qFinalExec = QueryExecutionFactory.create(q, model);
      QueryExecution qFinalExec = QueryExecutionFactory.create(qFinal, model);
      ResultSet rs = qFinalExec.execSelect();
      
      int cacheResultAmount = 0;
      
      while (rs.hasNext()) {
        rs.next();
        cacheResultAmount++;
      }
      
      long stop = System.nanoTime();
      String ar = "Time after reading all results: " + (stop - startLine);
      
      if (cacheResultAmount >= 0) {
        //System.out.println("FOUND ONE");
    	output += "Info for query number " + (queryNumber - 1) + '\n';
    	output += q.toString() + '\n';
        output += ap + '\n';
        output += bpf + '\n';
        output += gsq + '\n';
        output += brd + '\n';
        output += ard + '\n';
        output += nbgps + '\n';
        output += cbl + '\n';
        output += solution + '\n';
        output += bo + '\n';
        output += br + '\n';
        output += ar + '\n';
        output += "Origin: " + qu.split("\t")[3] + '\n';
        output += "Cache size is: " + myCache.cacheSize() + '\n';
        output += "Results size is: " + myCache.resultsSize() + '\n';
        output += "Query " + (queryNumber - 1) + " results with cache: " + cacheResultAmount + '\n';
        output += "Number of cache hits: " + myCache.getCacheHits() + '\n';
        output += "Number of retrievals: " + myCache.getRetrievalHits() + '\n';
        output += '\n';
      }
    } catch (InterruptedException | HashCollisionException e) {
    	w.println("Info for query number " + (queryNumber - 1)); 
    	e.printStackTrace(w);
        w.println();
    } 
    w.println(output);
    w.flush();
	model.commit();
  }
  
  public void readLog(File file) throws IOException {
    final PrintWriter w = new PrintWriter(new FileWriter("/home/gchandia/Thesis/" 
			 							  + file.getName().substring(file.getName().indexOf("F"), file.getName().length()) 
			 							  + "_Results.txt"));
	Parser p = new Parser();
	
	try (BufferedReader br = new BufferedReader(new FileReader(file))) {
	  while ((qu = br.readLine()) != null) {
		Query q = p.parseDbPedia(qu);
		Future<?> future = THREAD_POOL.submit(() -> processQuery(q, w));
        futures.add(future);
	  }
	} catch (IOException e) {e.printStackTrace();}
	
	for (Future<?> future : futures) {
	  try {
		future.get(15, TimeUnit.SECONDS);
	  } catch (InterruptedException | ExecutionException | TimeoutException e) {future.cancel(true);}
	}
	
	THREAD_POOL.shutdown();
	w.close();
  }
}
