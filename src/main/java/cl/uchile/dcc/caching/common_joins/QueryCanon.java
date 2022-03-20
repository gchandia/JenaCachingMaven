package cl.uchile.dcc.caching.common_joins;

import java.util.NoSuchElementException;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;

import cl.uchile.dcc.qcan.main.SingleQuery;


/**
 * All methods relating to canonicalizing queries are found here
 * @author gch12
 *
 */
public class QueryCanon {
	
	public Query getCanonQuery(Query q) throws Exception {
		SingleQuery sq = null;
		try {
			sq = new SingleQuery(q.toString(), true, true, false, true);			
		} catch (NoSuchElementException e) {}
		return QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
	}
}