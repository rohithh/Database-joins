package IEJoins;

import java.util.List;
import java.io.IOException;
import java.util.ArrayList;

import global.AttrOperator;
import global.TupleOrder;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import iterator.TupleUtilsException;

/**
 * 
 * @author jithin
 *
 */

/**
 * IE Join algorithm for double predicate, uses MemorySort class and SortObject class
 */
public class IEJoins {

	private FileScan am = null;
	private FileScan am2 = null;
	private readRelations rr;
	private readRelations rr2;

	private  int chunkSize = 5;
	private boolean[] bloomFilter ;
	
	int numOuterColumns;
	int numInnerColumns;
	
	String nameOuter;
	String nameInner;

	public IEJoins(queryUtils qu, readRelations relation1, readRelations relation2)
			throws FileScanException, TupleUtilsException, InvalidRelation, IOException {
		rr = relation1;
		rr2 = relation2;
		
		numOuterColumns = rr.getAttributeTypes().size();
		numInnerColumns = rr2.getAttributeTypes().size();
		nameOuter = qu.getOuterRelation();
		nameInner = qu.getInnerRelation();

		am = new FileScan(nameOuter + ".in", queryCondExpr.getAttrType(rr.getAttributeTypes(), numOuterColumns),
				queryCondExpr.getStrSizes(rr.getNumOfStringAtrr()), (short) numOuterColumns, (short) numOuterColumns,
				queryCondExpr.getOuterProjection(numOuterColumns), null);

		am2 = new FileScan(nameInner + ".in", queryCondExpr.getAttrType(rr2.getAttributeTypes(), numInnerColumns),
				queryCondExpr.getStrSizes(rr2.getNumOfStringAtrr()), (short) numInnerColumns, (short) numInnerColumns,
				queryCondExpr.getOuterProjection(numInnerColumns), null);

		
	}
	
	private List<String> ieJoinAlgo(boolean useBloom) {
		
		CondExpr[] condExprArray = queryCondExpr.getConditionExpr();
		FldSpec[] projection = queryCondExpr.getProjection();
		
		
		CondExpr firstCondition = condExprArray[0];
		CondExpr secondCondition = condExprArray[1];

		int sort_colNum_L1 = firstCondition.operand1.symbol.offset;
		int sort_colNum_L1_ = firstCondition.operand2.symbol.offset;

		int sort_colNum_L2 = secondCondition.operand1.symbol.offset;
		int sort_colNum_L2_ = secondCondition.operand2.symbol.offset;

		MemorySort sort_L1 = null;
		MemorySort sort_L1_ = null;
		MemorySort sort_L2 = null;
		MemorySort sort_L2_ = null;

		int[] permutation = new int[rr.numberOfRows];
		int[] permutation_ = new int[rr2.numberOfRows];

		int[] offset1 = new int[rr.numberOfRows];
		int[] offset2 = new int[rr.numberOfRows];

		//int[] bitArray = new int[rr2.numberOfRows];
        boolean[] bitArray = new boolean[rr2.numberOfRows];

        for (int index = 0; index < rr2.numberOfRows; index++) {
			bitArray[index] = false;
		}
		
		for (int index = 0; index <offset1.length; index++) {
			offset1[index] = -1;
		}

		for (int index = 0; index <offset2.length; index++) {
			offset2[index] = -1;
		}
		
		//List<SortObject> join_result = new ArrayList<SortObject>();
        List<String> join_result = new ArrayList<String>();
		/*System.out.println("Query = " + sort_colNum_L1 +" "+ condExprArray[0].op.toString() + " " + sort_colNum_L1_ + " AND " +
				sort_colNum_L2 +" "+ condExprArray[1].op.toString() + " " + sort_colNum_L2_);*/
		if (condExprArray[0].op.attrOperator == AttrOperator.aopGT
				|| condExprArray[0].op.attrOperator == AttrOperator.aopGE) {
			
			// Sort L1 and L1' in descending order
			
			sort_L1 = new MemorySort(am, sort_colNum_L1, new TupleOrder(TupleOrder.Descending),projection[0].offset);
			sort_L1_ = new MemorySort(am2, sort_colNum_L1_, new TupleOrder(TupleOrder.Descending),projection[1].offset);

		} else if (condExprArray[0].op.attrOperator == AttrOperator.aopLT
				|| condExprArray[0].op.attrOperator == AttrOperator.aopLE) {
			// Sort L1 and L1' in ascending order
			sort_L1 = new MemorySort(am, sort_colNum_L1, new TupleOrder(TupleOrder.Ascending),projection[0].offset);
			sort_L1_ = new MemorySort(am2, sort_colNum_L1_, new TupleOrder(TupleOrder.Ascending),projection[1].offset);
		}

		try {
			am = new FileScan(nameOuter + ".in", queryCondExpr.getAttrType(rr.getAttributeTypes(), numOuterColumns),
					queryCondExpr.getStrSizes(rr.getNumOfStringAtrr()), (short) numOuterColumns, (short) numOuterColumns,
					queryCondExpr.getOuterProjection(numOuterColumns), null);
			
			am2 = new FileScan(nameInner + ".in", queryCondExpr.getAttrType(rr.getAttributeTypes(), numInnerColumns),
					queryCondExpr.getStrSizes(rr2.getNumOfStringAtrr()), (short) numInnerColumns, (short) numInnerColumns,
					queryCondExpr.getOuterProjection(numInnerColumns), null);
		} catch (FileScanException e) {
			e.printStackTrace();
		} catch (TupleUtilsException e) {
			e.printStackTrace();
		} catch (InvalidRelation e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	
		if (condExprArray[1].op.attrOperator == AttrOperator.aopGT
				|| condExprArray[1].op.attrOperator == AttrOperator.aopGE) {
			// Sort L2 and L2' in ascending order
			sort_L2 = new MemorySort(am, sort_colNum_L2, new TupleOrder(TupleOrder.Ascending),projection[0].offset);
			sort_L2_ = new MemorySort(am2, sort_colNum_L2_, new TupleOrder(TupleOrder.Ascending),projection[1].offset);

		} else if (condExprArray[1].op.attrOperator == AttrOperator.aopLT
				|| condExprArray[1].op.attrOperator == AttrOperator.aopLE) {
			// Sort L2 and L2' in descending order
			sort_L2 = new MemorySort(am, sort_colNum_L2, new TupleOrder(TupleOrder.Descending),projection[0].offset);
			sort_L2_ = new MemorySort(am2, sort_colNum_L2_, new TupleOrder(TupleOrder.Descending),projection[1].offset);

		}

		List<SortObject> sortedL1 = sort_L1.sort();
		List<SortObject> sortedL1_ = sort_L1_.sort();
		List<SortObject> sortedL2 = sort_L2.sort();
		List<SortObject> sortedL2_ = sort_L2_.sort();

		// Optimization
		if (useBloom) {

			if (chunkSize >= sortedL1_.size()) {
				chunkSize = 2;
			}
			int bloomSize = sortedL1_.size() / chunkSize;
			if (sortedL1_.size() % chunkSize > 0) {
				bloomSize++;
			}
			bloomFilter = new boolean[bloomSize];
			for (int index = 0; index < bloomSize; index++) {
				bloomFilter[index] = false;
			}
		}

		for (int i = 0; i < sortedL2.size(); i++) {
			SortObject obj = sortedL2.get(i);
			
			for (int j = 0;j< sortedL1.size(); j++) { 
				
				SortObject subObj = sortedL1.get(j);
				if (obj.getRowIndex() == subObj.getRowIndex()) {
					permutation[i] = j;
					break;
				}
				
			}
		}
		
		for (int i = 0; i < sortedL2_.size(); i++) {
			SortObject obj = sortedL2_.get(i);
			
			for (int j = 0;j< sortedL1_.size(); j++) { 
				
				SortObject subObj = sortedL1_.get(j);
				if (obj.getRowIndex() == subObj.getRowIndex()) {
					permutation_[i] = j;
					break;
				}
				
			}
		}
		
		for (int i = 0; i < sortedL1.size(); i++) {
			SortObject obj = sortedL1.get(i);
			
			for (int j = 0;j< sortedL1_.size(); j++) {
				SortObject subObj = sortedL1_.get(j);
				if (obj.getSortValue() == subObj.getSortValue()) {
					offset1[i] = j;
					break;
				} else if (obj.getSortValue() > subObj.getSortValue()) {
					offset1[i] = j+1;
					
				}
				
			}
		}
		
		
		for (int i = 0; i < sortedL2.size(); i++) {
			SortObject obj = sortedL2.get(i);
			
			for (int j = 0;j< sortedL2_.size(); j++) {
				SortObject subObj = sortedL2_.get(j);
				if (obj.getSortValue() == subObj.getSortValue()) {
					offset2[i] = j;
					break;
				} else if (obj.getSortValue() > subObj.getSortValue()) {
					offset2[i] = j+1;
					
				}
				
			}
		}
		
		/*System.out.println("");
		System.out.println("Sorted L1:");
		for (int index = 0; index < sortedL1.size(); index++) {
			System.out.print("r"+sortedL1.get(index).rowIndex+"("+sortedL1.get(index).sortValue+") |");
		}
		
		System.out.println("");
		System.out.println("----------------------------");
		System.out.println("Sorted L2:");
		for (int index = 0; index < sortedL2.size(); index++) {
			System.out.print("r"+sortedL2.get(index).rowIndex+"("+sortedL2.get(index).sortValue+") |");
		}
		
		System.out.println("");
		System.out.println("----------------------------");
		System.out.println("P:");
		for (int index = 0; index < permutation.length; index++) {
			System.out.print(permutation[index]+" |");
		}
		
		System.out.println("");
		System.out.println("----------------------------");
		System.out.println("O1:");
		for (int index = 0; index < offset1.length; index++) {
			System.out.print(offset1[index]+" |");
		}
		
		System.out.println("");
		System.out.println("----------------------------");
		System.out.println("O2:");
		for (int index = 0; index < offset2.length; index++) {
			System.out.print(offset2[index]+" |");
		}
		
		System.out.println("");
		System.out.println("----------------------------");
		System.out.println("Sorted L1':");
		for (int index = 0; index < sortedL1_.size(); index++) {
			System.out.print("r"+sortedL1_.get(index).rowIndex+"("+sortedL1_.get(index).sortValue+") |");
		}
		
		System.out.println("");
		System.out.println("----------------------------");
		System.out.println("Sorted L2':");
		for (int index = 0; index < sortedL2_.size(); index++) {
			System.out.print("r"+sortedL2_.get(index).rowIndex+"("+sortedL2_.get(index).sortValue+") |");
		}
		
		System.out.println("");
		System.out.println("----------------------------");
		System.out.println("P':");
		for (int index = 0; index < permutation_.length; index++) {
			System.out.print(permutation_[index]+" |");
		}
		System.out.println("");
		System.out.println("----------------------------");*/

		int eqOff = 1;
		if (condExprArray[0].op.attrOperator == AttrOperator.aopGE ||condExprArray[0].op.attrOperator == AttrOperator.aopLE
				&&condExprArray[1].op.attrOperator == AttrOperator.aopGE ||condExprArray[1].op.attrOperator == AttrOperator.aopLE) {
			eqOff = 0;
		}
		//Apply the algorithm to find the joins
		for (int index = 0; index < sortedL2.size(); index++) {
			//System.out.println("visit L2["+index+"] for row index:"+ sortedL2.get(index).rowIndex );
			
			//off2 ← O2[i]
			int off2 = offset2[index];
			
			//for j ← 1 to O2[i] do
				//B′[P′[j]] ← 1
			//System.out.println("Off2 - " + off2);
			for (int j = 0; j <= off2; j++) {
				
				int arrayPosition = permutation_[j];
				bitArray[arrayPosition] = true;
			
				if (useBloom) {
					bloomFilter[arrayPosition/chunkSize] = true;
				}
			}
		
			for (int offIdx = 0; offIdx < rr2.numberOfRows; offIdx++) {
				//System.out.print(bitArray[offIdx]+" |");
			}
			//System.out.println("");
			//off1 ← O1[P[i]]
			int off1 = offset1[permutation[index]];
			//for (k ← off1 + eqOff to n) do
				//if B′[j] = 1 then
					//add tuples w.r.t. (L2[i],L′2[k]) to join result
			
			//System.out.println("-- Join result --");
			if (useBloom) {
				
				int posOfBinaryArray = off1 + eqOff;
				int posOfBloomArray = posOfBinaryArray/chunkSize;
				
				//System.out.println("posOfBloomArray:" + posOfBloomArray);
				// Find true items in bloom filter.
				for (int bloomIndex = posOfBloomArray; bloomIndex < bloomFilter.length; bloomIndex++) {
					
					int endIndex = bloomIndex * chunkSize + chunkSize;
					
					if (bloomIndex == bloomFilter.length - 1 
							&& endIndex > bitArray.length) {
						endIndex = bloomIndex * chunkSize + bitArray.length%chunkSize;
					}
					
//					System.out.println("Start: " + bloomIndex * chunkSize);
//					System.out.println("End: " + endIndex);
					// From the position of bloom filter find all item with 1 in binary array.
					for (int k = bloomIndex * chunkSize; k < endIndex && bloomFilter[bloomIndex]; k ++) {
						if (k < 0 ) continue;
						
						if (bitArray[k] && k >= posOfBinaryArray) {
							
							System.out.println("["+sortedL2.get(index).projColValue+","+sortedL1_.get(k).projColValue+"]");
							join_result.add("["+sortedL2.get(index).projColValue+","+sortedL1_.get(k).projColValue+"]");
						}
					}
				}
				
			} else {
				for (int k = off1 + eqOff; k < sortedL2_.size() ; k ++) {
					if (k < 0 ) continue;
					if (bitArray[k] ) {
						
						System.out.println("["+sortedL2.get(index).projColValue+","+sortedL1_.get(k).projColValue+"] ");
						join_result.add("["+sortedL2.get(index).projColValue+","+sortedL1_.get(k).projColValue+"] ");
					}
				}
			}
			
//			System.out.println("Bit Array:");
//			for (int offIdx = 0; offIdx < bitArray.length; offIdx++) {
//				System.out.print(bitArray[offIdx]+" |");
//			}
//			System.out.println("");
//			System.out.println("Bloom array");
//			for (int offIdx = 0; offIdx < bloomFilter.length; offIdx++) {
//				System.out.print(bloomFilter[offIdx]+" |");
//			}
//			System.out.println("");
			
		}
        //System.out.println(join_result.size());
        
        
        return join_result;
    }
	

	public List<String> joinWithQuery() {
		return ieJoinAlgo(false);

	}
	
	public List<String> joinWithOptimizedQuery() {
		return ieJoinAlgo(true);
	}
}
