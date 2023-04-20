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
  
  public void sendRequest(String input, int queryNumber, String cacheFile, String bgpsFile) {
	LogReader r = new LogReader(myCache, myBgpSubQueries);
	//Better to comment this line if not going to load anything
	//r.loadCache(cacheFile);
	r.setQueryNumber(queryNumber);
	try {
	  r.readLog(new File(input));
	} catch (IOException e) {
		e.printStackTrace();
	} catch (Exception e) {
		e.printStackTrace();
	}
	r.writeSubQueries(bgpsFile);
  }
  
  public static void main(String[] args) {
	Controller c = new Controller();
	c.sendRequest("/home/gchandia/wikidata_logs/FilteredLogs_1.tsv", 1, "", "/home/gchandia/Thesis/myBgpSubQueries.tsv");
	myCache.dumpCache("/home/gchandia/Thesis/Cache.tsv");
	//c.sendRequest("/home/gchandia/wikidata_logs/FilteredLogs_2.tsv", 50001, "/home/gchandia/Thesis/Cache.tsv");
	//myCache.dumpCache("/home/gchandia/Thesis/Cache2.tsv");
	//c.sendRequest("/home/gchandia/wikidata_logs/FilteredLogs_3.tsv", 100001);
	//c.sendRequest("/home/gchandia/wikidata_logs/FilteredLogs_4.tsv", 150001);
  }
}
