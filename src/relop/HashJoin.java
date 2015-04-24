package relop;

import java.util.ArrayList;
import java.util.Random;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;


/**
 * Implements the hash-based join algorithm described in section 14.4.3 of the
 * textbook (3rd edition; see pages 463 to 464). HashIndex is used to partition
 * the tuples into buckets, and HashTableDup is used to store a partition in
 * memory during the matching phase.
 */
public class HashJoin extends Iterator {

  /**
   * Constructs a hash join, given the left and right iterators and which
   * columns to match (relative to their individual schemas).
   */
	private static final String CHAR_LIST = 
	        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
	private Iterator _left;
	private Iterator _right;
	private Integer _lcol;
	private Integer _rcol;
	private IndexScan _lbucket;
	private IndexScan _rbucket;
	private Tuple nextTuple;
	
	ArrayList<String> _tuples;
	
	IndexScan innerIndexScan;
	IndexScan outerIndexScan;

	private HashTableDup _hashtable;
	
  public HashJoin(Iterator left, Iterator right, Integer lcol, Integer rcol) {
	  _left = left;
	  _right = right;
	  _lcol = lcol;
	  _rcol = rcol;
	  schema = Schema.join(left.schema, right.schema);
	  _tuples = new ArrayList<String>();
	  
	  _hashtable = new HashTableDup();
	  currentIndex = -1;
	  currentRight = null;
	  FileScan lfScan = convertFileScan(left);
	  FileScan rfScan = convertFileScan(right);
	  HashIndex lh = new HashIndex(null);
	  while(lfScan.hasNext()) {
		  lh.insertEntry(new SearchKey(lfScan.getNext().getField(lcol)), lfScan.getLastRID());
	  }
	  outerIndexScan = new IndexScan(left.schema, lh, lfScan._file);
	  HashIndex rh = new HashIndex(null);
	  while(rfScan.hasNext()) {
		  rh.insertEntry(new SearchKey(rfScan.getNext().getField(rcol)), rfScan.getLastRID());
	  }
	  innerIndexScan = new IndexScan(right.schema, rh, rfScan._file);
	  
	  makeHashTable();
	  outerIndexScan.restart();
  }
  
  
  public void printIndexScan() {
	  System.out.println("Left");
	  while(outerIndexScan.hasNext()) {
		  Tuple tuple = outerIndexScan.getNext();
		  tuple.print();
	  }
  }
  private int getRandomNumber() {
      int randomInt = 0;
      Random randomGenerator = new Random();
      randomInt = randomGenerator.nextInt(CHAR_LIST.length());
      if (randomInt - 1 == -1) {
          return randomInt;
      } else {
          return randomInt - 1;
      }
  }
  
  public FileScan convertFileScan(Iterator iter){
      FileScan f;
      StringBuffer randStr = new StringBuffer();
      for(int i=0; i<16; i++){
          int number = getRandomNumber();
          char ch = CHAR_LIST.charAt(number);
          randStr.append(ch);
      }
      if(iter instanceof IndexScan){
   	   f = new FileScan(iter.schema, ((IndexScan) iter)._file);
      } else{
   	   HeapFile file = new HeapFile(randStr.toString());
   	   while(iter.hasNext()){
   		   file.insertRecord(iter.getNext().data);
   	   }
   	   f = new FileScan(iter.schema, file);
      }
      
      return f;
  }
  
  public void makeHashTable() {
	  while(outerIndexScan.hasNext()) {
		  Tuple tuple = outerIndexScan.getNext();
		  SearchKey hash = new SearchKey(tuple.getField(_lcol).toString());
		  _hashtable.add(hash, tuple);
	  }
  }
  

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    System.out.println("HashJoin : Iterator");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  _tuples = new ArrayList<String>();
    outerIndexScan.restart();
    innerIndexScan.restart();
    currentRight = null;
    currentIndex = -1;
    makeHashTable();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return (_lbucket.isOpen() && _rbucket.isOpen());
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  outerIndexScan.close();
	  innerIndexScan.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  private Tuple currentRight;
  private int currentIndex;
  public boolean hasNext() {
	  //printIndexScan();
	  if(currentIndex == -1) {
		  try{
		  currentRight = innerIndexScan.getNext();
		  } catch (IllegalStateException e) {
			  nextTuple = null;
			  return false;
		  }
		  currentIndex = 0;
	  }
	  
	  SearchKey key = new SearchKey(currentRight.getField(_rcol).toString());
	  Tuple[] tuples = _hashtable.getAll(key);
	  if(tuples == null) {
		  //System.out.println("NULL");
		  currentIndex = -1;
		  return hasNext();
	  }
	  if(currentIndex >= tuples.length) {
		  //System.out.println("OUT");
		  currentIndex = -1;
		  return hasNext();
	  }
	  //System.out.println("OK");
	  Tuple leftTuple = tuples[currentIndex];
	  nextTuple = Tuple.join(leftTuple, currentRight, schema);
	  currentIndex++;
	  if(!_tuples.contains(new String(nextTuple.data))) {
		  _tuples.add(new String(nextTuple.data));
		  return true;
	  } else {
		  //System.out.println("Found dup");
		  return hasNext();
	  }
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if(nextTuple == null) {
    	throw new IllegalStateException("No more tuples");
    } else {
    	return nextTuple;
    }
  }

} // public class HashJoin extends Iterator
