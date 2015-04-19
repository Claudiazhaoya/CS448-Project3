package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

  /**
   * Constructs an index scan, given the hash index and schema.
   */
	private HashIndex _index;
	private SearchKey _key;
	private HeapFile _file;
	private HashScan _scanner;
	private boolean _isOpen;
  public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
	  this.schema = schema;
	  _index = index;
	  _key = key;
	  _file = file;
	  _scanner = _index.openScan(_key);
	  _isOpen = true;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    System.out.println("KeyScan : Iterator");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    close();
    _scanner = _index.openScan(_key);
    _isOpen = true;
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
    byte[] data = _file.selectRecord(_scanner.getNext());
    if(data == null)
    	throw new IllegalStateException("No More Tuples");
    return new Tuple(schema, data);
  }

} // public class KeyScan extends Iterator
