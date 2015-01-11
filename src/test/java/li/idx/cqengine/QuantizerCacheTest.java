package li.idx.cqengine;

import static com.googlecode.cqengine.query.QueryFactory.and;
import static com.googlecode.cqengine.query.QueryFactory.equal;
import static com.googlecode.cqengine.query.QueryFactory.in;
import static com.googlecode.cqengine.query.QueryFactory.lessThan;
import static li.idx.cqengine.base.CacheTestHelper.fillCache;
import static li.idx.cqengine.base.CacheTestHelper.search;

import java.util.Arrays;

import li.idx.cqengine.base.Item;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.googlecode.cqengine.CQEngine;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.hash.HashIndex;
import com.googlecode.cqengine.index.navigable.NavigableIndex;
import com.googlecode.cqengine.index.unique.UniqueIndex;
import com.googlecode.cqengine.quantizer.LongQuantizer;
import com.googlecode.cqengine.query.Query;

@RunWith(Parameterized.class)
public class QuantizerCacheTest {
	
	@Parameters
	public static Iterable<Object[]> compressionFactors() {
		return Arrays.asList(
			param(0), 
			param(10), 
			param(1000),
			param(10_000), 
			param(50_000), 
			param(100_000), 
			param(150_000), 
			param(200_000));
	}

	@Parameter
	public int compressionFactor;

	private static Object[] param(Object arg) {
		return new Object[]{arg};
	}

	public IndexedCollection<Item> createCache() {
		IndexedCollection<Item> items = CQEngine.newInstance();

		items.addIndex(UniqueIndex.onAttribute(Item.ID));
		items.addIndex(HashIndex.onAttribute(Item.TYPE));
		items.addIndex(HashIndex.onAttribute(Item.TAGS));

		if (compressionFactor >= 2) {
			System.out.println("\n### Run test with compression factor " + compressionFactor + " on NavigableIndex on Item.CREATED");
			items.addIndex(NavigableIndex.withQuantizerOnAttribute(LongQuantizer.withCompressionFactor(compressionFactor), Item.CREATED));
		}else{
			System.out.println("\n### Run test normal NavigableIndex on Item.CREATED");
			items.addIndex(NavigableIndex.onAttribute(Item.CREATED));
		}

		return fillCache(items, 150_000);
	}

	@Test(timeout = 2_000)
	public void testJumpIn() throws Exception {
		IndexedCollection<Item> items = createCache();

		final int CREATED_START = 50000;
		final int STEPS = 11;
		final int STEP_SIZE = 10000;

		System.out.println("Size tag query: " + items.retrieve(in(Item.TAGS, "t/a", "x")).size());
		System.out.println("Size created query: " + items.retrieve(lessThan(Item.CREATED, 68000L)).size());

		for (long i = 0; i < STEPS; i++) {
			Query<Item> query = and(equal(Item.TYPE, "a"), lessThan(Item.CREATED, CREATED_START + i * STEP_SIZE), in(Item.TAGS, "t/a", "x"));
			System.out.println("Query: " + query);
			search(items, query);
		}
	}
}
