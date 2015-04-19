package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import heap.HeapScan;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {

  /**
   * Constructs an index scan, given the hash index and schema.
   */
	
	//private Schema _schema;
	private BucketScan _scanner;
	private HeapFile _file;
	private RID		_rid;
	private boolean _isOpen;
	private HashIndex _index;
	
	
	
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
	  this.schema = schema;
	    _file = file;
	    _index = index;
	    _scanner = index.openScan();
	    _isOpen = true;
	    _rid = null;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  System.out.println("IndexScan : Iterator");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  _scanner.close();
	    _scanner = _index.openScan();
	    _rid = null;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  return _isOpen;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  _isOpen = false;
	  _scanner.close();
	  _rid = null;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  return _scanner.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  _rid = new RID();
	    _rid = _scanner.getNext();
	    if( _rid == null) throw new IllegalStateException("IllegalStateException");
	    
	    byte [] data = _file.selectRecord(_rid);
	    if( data == null) throw new IllegalStateException("IllegalStateException");
	    Tuple tuple = new Tuple(schema, data);
	    return tuple;
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
    return _scanner.getLastKey();
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
    return _scanner.getNextHash();
  }

} // public class IndexScan extends Iterator
