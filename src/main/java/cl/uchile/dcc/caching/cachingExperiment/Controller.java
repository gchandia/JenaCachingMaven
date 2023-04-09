package cl.uchile.dcc.caching.cachingExperiment;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.jena.sparql.algebra.op.OpBGP;

import cl.uchile.dcc.caching.cache.Cache;
import cl.uchile.dcc.caching.cache.CustomCacheV5;

public class Controller {
  private static Cache myCache;
  //Keeps last bgps that have been checked
  private static ArrayList<OpBGP> myBgpSubQueries;
  
  public Controller() {
	myCache = new CustomCacheV5(1000, 10000000, 900, 10);
	myBgpSubQueries = new ArrayList<OpBGP>();
  }
  
  public void sendRequest(String input, int queryNumber) {
	LogReader r = new LogReader(myCache, myBgpSubQueries);
	r.setQueryNumber(queryNumber);
	try {
	  r.readLog(new File(input));
	} catch (IOException e) {e.printStackTrace();}
	myCache = r.getCache();
	myBgpSubQueries = r.getMyBgpSubQueries();
  }
  
  public static void main(String[] args) {
	Controller c = new Controller();
	c.sendRequest("/home/gchandia/wikidata_logs/FilteredLogs_1.tsv", 1);
	//c.sendRequest("/home/gchandia/wikidata_logs/FilteredLogs_2.tsv", 50001);
	//c.sendRequest("/home/gchandia/wikidata_logs/FilteredLogs_3.tsv", 100001);
	//c.sendRequest("/home/gchandia/wikidata_logs/FilteredLogs_4.tsv", 150001);
  }
}
