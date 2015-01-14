package li.idx.cqengine;

import static com.googlecode.cqengine.query.QueryFactory.ascending;
import static com.googlecode.cqengine.query.QueryFactory.lessThan;
import static com.googlecode.cqengine.query.QueryFactory.orderBy;
import static com.googlecode.cqengine.query.QueryFactory.queryOptions;
import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import li.idx.cqengine.base.Item;

import org.junit.Test;

import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.query.simple.LessThan;
import com.googlecode.cqengine.resultset.ResultSet;

/**
 * The following tests demonstrate that the result for a given can depend on the size of the quantizer.
 * @author asigner
 */
public class QueryWithQuantizerAccuracyTest {

	@Test
	public void testWithSmallCompressionFactor() throws Exception {
		final int COMPRESSION_FACTOR = 2;

		testRetrievWithQuantizer(COMPRESSION_FACTOR);
	}

	@Test
	public void testWithBigCompressionFactor() throws Exception {
		final int COMPRESSION_FACTOR = 5;

		testRetrievWithQuantizer(COMPRESSION_FACTOR);
	}

	private void testRetrievWithQuantizer(final int COMPRESSION_FACTOR) {
		IndexedCollection<Item> items = QuantizerCacheTest.createCache(15, COMPRESSION_FACTOR);

		LessThan<Item, Long> query = lessThan(Item.CREATED, 6L);
		System.out.println("Query: " + query);
		
		ResultSet<Item> retrieve = items.retrieve(query, queryOptions(orderBy(ascending(Item.ID))));
		dump(retrieve);

		assertEquals(6, retrieve.size());
	}

	private void dump(ResultSet<Item> retrieve) {
		Iterator<Item> iter = retrieve.iterator();
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
		System.out.println("Total size: " + retrieve.size());
	}
}