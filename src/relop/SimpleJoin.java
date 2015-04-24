package relop;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {

  /**
   * Constructs a join, given the left and right iterators and join predicates
   * (relative to the combined schema).
   */
	private Iterator _left;
	private Iterator _right;
	private Predicate[] _preds;
	private boolean _isOpen;
  public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
    _left = left;
    _right = right;
    this.schema = Schema.join(left.schema, right.schema);
    _preds = preds;
    _isOpen = true;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    System.out.println("SimpleJoin : Iterator");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    _left.restart();
    _right.restart();
    ltuple = null;
    rtuple = null;
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
    _left.close();
    _right.close();
    _isOpen = false;
    ltuple = null;
    rtuple = null;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return _left.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  private Tuple ltuple = null;
  private Tuple rtuple = null;
  public Tuple getNext() {
	while(_left.hasNext()){
		ltuple = _left.getNext();
		while(_right.hasNext()) {
	    	//Keep using the same left
	    	rtuple = _right.getNext();
	    	Tuple tuple = Tuple.join(ltuple, rtuple, schema);
	    	if(validate(tuple))
	    		return tuple;
	    }
		
	}
	throw new IllegalStateException("No more tuples");
    
  }
  public boolean validate(Tuple tuple) {
	  for(Predicate p : _preds) {
	    	if(p.evaluate(tuple) == true) {
	    		return true;
	    	}
	    }
	  return false;
  }

} // public class SimpleJoin extends Iterator
