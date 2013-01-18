package snippet;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class itera {
	public static void main(String[] args){
		List<Integer> l = new LinkedList<Integer>();
		for(int i=0;i<10;i++){
			l.add(i);
		}
		Iterator it  = l.listIterator();
		System.out.println(count(it));
		System.out.println(count(it));
	}
	
	public static int count(final Iterable iterable) {
        return count(iterable.iterator());
    }
	public static int count(final Iterator iterator) {
        int counter = 0;
        while (iterator.hasNext()) {
            iterator.next();
            counter++;
        }
        return counter;
    }
}
