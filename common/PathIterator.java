package common;

import java.util.Iterator;

/**
 * Created by tao on 5/7/16.
 */
public class PathIterator implements Iterator<String>{

    Iterator<String> iterator;

    public PathIterator(Iterator<String> itr){
        this.iterator = itr;
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public String next() {
        return this.iterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cannot remove on this iterator");
    }
}
