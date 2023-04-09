package cl.uchile.dcc.caching.cachingExperiment;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.jena.sparql.algebra.op.OpBGP;

import cl.uchile.dcc.caching.cache.Cache;
import cl.uchile.dcc.caching.cache.CustomCacheV5;

public class Controller {
  private static Cache myCache;
  //Keeps last bgps in one query that have been checked by the function
  private static ArrayList<OpBGP> checkedBgpSubQueries;
  //Keeps last bgps that have been checked
  private static ArrayList<OpBGP> myBgpSubQueries;
  //Keeps last bgps that were attempted to be cached in one query
  private static ArrayList<OpBGP> cachedBgpSubQueries;
  
  public Controller() {
	myCache = new CustomCacheV5(1000, 10000000, 900, 10);
	checkedBgpSubQueries = new ArrayList<OpBGP>();
	myBgpSubQueries = new ArrayList<OpBGP>();
	cachedBgpSubQueries = new ArrayList<OpBGP>();
  }
  
  public void sendRequest(String input, int queryNumber) {
	LogReader r = new LogReader(myCache, checkedBgpSubQueries, myBgpSubQueries, cachedBgpSubQueries);
	r.setQueryNumber(queryNumber);
	try {
	  r.readLog(new File(input));
	} catch (IOException e) {e.printStackTrace();}
  }
  
  public static void main(String[] args) {
	Controller c = new Controller();
	c.sendRequest("/home/gchandia/wikidata_logs/FilteredLogs_1.tsv", 1);
	c.sendRequest("/home/gchandia/wikidata_logs/FilteredLogs_2.tsv", 50001);
	c.sendRequest("/home/gchandia/wikidata_logs/FilteredLogs_3.tsv", 100001);
	c.sendRequest("/home/gchandia/wikidata_logs/FilteredLogs_4.tsv", 150001);
  }
}
