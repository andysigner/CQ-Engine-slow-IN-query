package li.idx.cqengine;

import static com.googlecode.cqengine.query.QueryFactory.and;
import static com.googlecode.cqengine.query.QueryFactory.equal;
import static com.googlecode.cqengine.query.QueryFactory.in;
import static com.googlecode.cqengine.query.QueryFactory.lessThan;
import static li.idx.cqengine.base.CacheTestHelper.fillCache;
import static li.idx.cqengine.base.CacheTestHelper.logCurrentMethodName;
import static li.idx.cqengine.base.CacheTestHelper.logItemCountByQuery;
import static li.idx.cqengine.base.CacheTestHelper.search;
import li.idx.cqengine.base.Item;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.googlecode.cqengine.CQEngine;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.hash.HashIndex;
import com.googlecode.cqengine.index.navigable.NavigableIndex;
import com.googlecode.cqengine.index.unique.UniqueIndex;
import com.googlecode.cqengine.query.Query;

/**
 * See https://groups.google.com/forum/#!topic/cqengine-discuss/y596gHRtOvE
 * @author asigner
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CacheTest {

	private static final IndexedCollection<Item> items = CQEngine.newInstance();

	@BeforeClass
	public static void init() {
		items.addIndex(UniqueIndex.onAttribute(Item.ID));
		items.addIndex(HashIndex.onAttribute(Item.TYPE));
		items.addIndex(HashIndex.onAttribute(Item.TAGS));
		items.addIndex(NavigableIndex.onAttribute(Item.CREATED));

		fillCache(items, 150_000);
	}

	@Test
	public void test1Fast() throws Exception {
		logCurrentMethodName();
		logItemCountByQuery(items, in(Item.TAGS, "t/a", "x"));

		Query<Item> query = and(equal(Item.TYPE, "a"), lessThan(Item.CREATED, 64286L), in(Item.TAGS, "t/a", "x"));
		System.out.println("Query:  " + query);

		search(items, query);
	}

	@Test(timeout = 2000)
	public void test2Slow() throws Exception {
		logCurrentMethodName();

		Query<Item> query = and(equal(Item.TYPE, "a"), lessThan(Item.CREATED, 64287L), in(Item.TAGS, "t/a", "x"));
		System.out.println("Query:  " + query);

		search(items, query);
	}
}
