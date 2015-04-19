package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

  /**
   * Constructs a file scan, given the schema and heap file.
   */
	private Schema _schema;
	private HeapScan _scanner;
	private HeapFile _file;
	private RID		_rid;
	private boolean _isOpen;
  public FileScan(Schema schema, HeapFile file) {
    _schema = schema;
    _file = file;
    _scanner = _file.openScan();
    _isOpen = true;
    _rid = null;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    System.out.println("FileScan : Iterator");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    _scanner.close();
    _scanner = _file.openScan();
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
    byte[] data = _scanner.getNext(_rid);
    Tuple tuple = new Tuple(_schema, data);
    return tuple;
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
    return _rid;
  }

} // public class FileScan extends Iterator
