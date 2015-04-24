package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
	private boolean _isOpen;
	private Iterator _iter;
	private Integer [] _fields;
	private Schema _newSchema;
	
	
	
	
	
  public Projection(Iterator iter, Integer... fields) {
    _isOpen = true;
    this.schema = iter.schema;
    _iter = iter;
    _iter.restart();
    _fields = fields;
    _newSchema = new Schema(_fields.length);
	  for(int i = 0;i<_fields.length;i++){
		  _newSchema.initField(i, schema.fieldType(_fields[i]), schema.fieldLength(_fields[i]), schema.fieldName(_fields[i]));
	  }
	  this.schema = _newSchema;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  System.out.println("Projection : Iterator");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
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
    _isOpen = false;
    _iter.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return _iter.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	Tuple tuple = new Tuple(_newSchema);
    Tuple tmptuple = _iter.getNext();
    
    for(int i = 0;i<_fields.length;i++){
    	tuple.setField(i, tmptuple.getField(_fields[i]));
    }
    
    
    return tuple;
  }

} // public class Projection extends Iterator
