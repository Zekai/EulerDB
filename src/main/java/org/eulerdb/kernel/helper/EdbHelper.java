package org.eulerdb.kernel.helper;

import java.util.Iterator;

import com.sleepycat.je.Transaction;
import com.tinkerpop.blueprints.CloseableIterable;

public class EdbHelper {
    public static int count(final Iterator iterator) {
        int counter = 0;
        while (iterator.hasNext()) {
            iterator.next();
            counter++;
        }
        return counter;
    }

    public static int count(final Iterable iterable) {
        return count(iterable.iterator());
    }

    public static int count(final CloseableIterable iterable) {
        return count(iterable.iterator());
    }
    
    public static Long getTransactionId(Transaction tx){
		return tx==null?0:tx.getId();
	}
}
