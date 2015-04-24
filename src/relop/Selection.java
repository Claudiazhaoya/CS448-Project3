package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
	private Predicate[] _preds;
	private Tuple _nextTuple;
	private Iterator _iter;
	private boolean _isOpen;
  public Selection(Iterator iter, Predicate... preds) {
    this._preds = preds;
    this._iter = iter;
    this.schema = iter.schema;
    this._iter.restart();
    this._isOpen = true;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    System.out.println("Selection : Iterator");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    close();
    _iter.restart();
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
	_iter.close();
    _isOpen = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  while (_iter.hasNext()) {
			Tuple tuple = _iter.getNext();
			if(validate(tuple)) {
				_nextTuple = tuple;
				return true;
			}
		}
	_nextTuple = null;
    return false;
  }
  
  public boolean hasNextRaw() {
	  return _iter.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	if(_nextTuple != null) {
		return _nextTuple;
	}
	else
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

} // public class Selection extends Iterator
