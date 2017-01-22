package IEJoins;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import bufmgr.PageNotReadException;

import global.AttrOperator;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.JoinsException;
import iterator.PredEvalException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.WrongPermat;

public class IESelfJoin {

	private queryUtils query = null;

	private readRelations rel = null;

	private FileScan attr1 = null;
	ArrayList<SortObject> L1 = new ArrayList<SortObject>();
	ArrayList<SortObject> L2 = new ArrayList<SortObject>();

	private String relName;
	private int numCol = 0;
	private ArrayList<ArrayList<Integer>> projectionFields = null;

	public IESelfJoin(queryUtils q, readRelations r1)
			throws IOException,
			FileScanException,
			TupleUtilsException,
			InvalidRelation, JoinsException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat
	{
		this.query = q;
		this.rel = r1;

		this.relName = query.getOuterRelation();
		List<String> attrTypesOfRel = rel.getAttributeTypes();
		this.numCol = attrTypesOfRel.size();
		this.attr1 = new FileScan(relName + ".in", queryCondExpr.getAttrType(attrTypesOfRel, numCol),
				queryCondExpr.getStrSizes(rel.getNumOfStringAtrr()), (short)numCol, (short)numCol,
				queryCondExpr.getOuterProjection(numCol), null);
		CondExpr[] exprArray = queryCondExpr.getConditionExpr();
		CondExpr firstCondition = exprArray[0];
		CondExpr secCondition = exprArray[1];
		int col1Sort_First = firstCondition.operand1.symbol.offset;
		int col1Sort_Second = secCondition.operand1.symbol.offset;
		FldSpec[] projFields = queryCondExpr.getProjection();
		projectionFields = readValues(this.attr1, projFields, col1Sort_First, col1Sort_Second);
	}

	public void selfJoin()
			throws JoinsException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat, IOException
	{
		int relSize = rel.numberOfRows;
		CondExpr[] exprArray = queryCondExpr.getConditionExpr();
		CondExpr firstCondition = exprArray[0];
		CondExpr secCondition = exprArray[1];

		/*System.out.println("------------Operator------------------");
		System.out.println(firstCondition.op.attrOperator +", "+ secCondition.op.attrOperator);
		System.out.println("--------------------------------------");
*/
		//get the projection fields

		if(secCondition != null && (secCondition.op.attrOperator == AttrOperator.aopGT ||
				secCondition.op.attrOperator == AttrOperator.aopGE))
		{
			sortArrayList(L2, new TupleOrder(TupleOrder.Descending));
		}
		else if(secCondition != null && (secCondition.op.attrOperator == AttrOperator.aopLE ||
				secCondition.op.attrOperator == AttrOperator.aopLT))
		{
			sortArrayList(L2, new TupleOrder(TupleOrder.Ascending));
		}
		if(firstCondition.op.attrOperator == AttrOperator.aopGT ||
				firstCondition.op.attrOperator == AttrOperator.aopGE)
		{
			sortArrayList(L1, new TupleOrder(TupleOrder.Ascending));
		}
		else if(firstCondition.op.attrOperator == AttrOperator.aopLE ||
				firstCondition.op.attrOperator == AttrOperator.aopLT)
		{
			sortArrayList(L1, new TupleOrder(TupleOrder.Descending));
		}

		/*
		System.out.println("L1 ArrayList Elements");
		for(int i=0; i<L1.size(); i++)
		{
			System.out.println(L1.get(i).sortValue);
		}
		System.out.println("L2 ArrayList Elements");
		for(int i=0; i<L2.size(); i++)
		{
			System.out.println(L2.get(i).sortValue);
		}
		System.out.println("------------------------------------");
		*/
		int[] permutArray = new int[L1.size()];
		// compute permutation array
		for(int i=0; i< L2.size(); i++)
		{
			SortObject obj = L2.get(i);
			for(int j=0; j< L1.size(); j++)
			{
				SortObject subObj = L1.get(j);
				if(obj.getRowIndex() == subObj.getRowIndex())
				{
					permutArray[i] = j;
					break;
				}
			}
		}
		/*
		System.out.println("-----------------Permutation Array-----");
		for(int i=0; i<permutArray.length; i++)
		{
			System.out.println(permutArray[i]);
		}
		System.out.println("-----------------------------------------");
		*/
		int eqOff = 1;

		boolean[] B = new boolean[L1.size()];
		for(int i=0; i < L1.size(); i++)
		{
			B[i] = false;
		}

		ArrayList<ArrayList<SortObject>> selfJoin_result = new ArrayList<ArrayList<SortObject>>();
		if(firstCondition.op.attrOperator == AttrOperator.aopGE || firstCondition.op.attrOperator == AttrOperator.aopLE
				&& secCondition.op.attrOperator == AttrOperator.aopGE || secCondition.op.attrOperator == AttrOperator.aopLE)
		{
			eqOff = 0;
		}

		//System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
		int pos = 0;

		for(int i=0; i < L1.size(); i++)
		{
			pos = permutArray[i];
			B[pos] = true;

			for(int j = pos+eqOff; j < L1.size(); j++)
			{
				ArrayList<SortObject> al = new ArrayList<SortObject>();
				if(B[j] && j >= pos+eqOff)
				{
					al.add(L1.get(j));
					al.add(L1.get(permutArray[i]));
					selfJoin_result.add(al);
				}
			}
		}

		// printing out the results of the self join
		//System.out.println("Result Tuple List Size "+selfJoin_result.size());
		for(int k=0; k<selfJoin_result.size(); k++)
		{
			ArrayList<SortObject> temp = selfJoin_result.get(k);
			System.out.print("[");
			printList(projectionFields.get(temp.get(0).rowIndex));
			System.out.print(", ");
			printList(projectionFields.get(temp.get(1).rowIndex));
			System.out.println("]");
		}
//		System.out.println("------------------------------------------");
	}

	public void optimizedSelfJoin()
			throws JoinsException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat, IOException
	{
		int relSize = rel.numberOfRows;
		CondExpr[] exprArray = queryCondExpr.getConditionExpr();
		CondExpr firstCondition = exprArray[0];
		CondExpr secCondition = exprArray[1];

		/*System.out.println("------------Operator------------------");
		System.out.println(firstCondition.op.attrOperator +", "+ secCondition.op.attrOperator);
		System.out.println("--------------------------------------");*/

		//get the projection fields
		if(secCondition != null && (secCondition.op.attrOperator == AttrOperator.aopGT ||
				secCondition.op.attrOperator == AttrOperator.aopGE))
		{
			sortArrayList(L2, new TupleOrder(TupleOrder.Descending));
		}
		else if(secCondition != null && (secCondition.op.attrOperator == AttrOperator.aopLE ||
				secCondition.op.attrOperator == AttrOperator.aopLT))
		{
			sortArrayList(L2, new TupleOrder(TupleOrder.Ascending));
		}
		if(firstCondition.op.attrOperator == AttrOperator.aopGT ||
				firstCondition.op.attrOperator == AttrOperator.aopGE)
		{
			sortArrayList(L1, new TupleOrder(TupleOrder.Ascending));
		}
		else if(firstCondition.op.attrOperator == AttrOperator.aopLE ||
				firstCondition.op.attrOperator == AttrOperator.aopLT)
		{
			sortArrayList(L1, new TupleOrder(TupleOrder.Descending));
		}

		/*
		System.out.println("L1 ArrayList Elements");
		for(int i=0; i<L1.size(); i++)
		{
			System.out.println(L1.get(i).sortValue);
		}
		System.out.println("L2 ArrayList Elements");
		for(int i=0; i<L2.size(); i++)
		{
			System.out.println(L2.get(i).sortValue);
		}
		System.out.println("------------------------------------");
		*/
		int[] permutArray = new int[L2.size()];
		// compute permutation array
		for(int i=0; i< L2.size(); i++)
		{
			SortObject obj = L2.get(i);
			for(int j=0; j< L1.size(); j++)
			{
				SortObject subObj = L1.get(j);
				if(obj.getRowIndex() == subObj.getRowIndex())
				{
					permutArray[i] = j;
					break;
				}
			}
		}
		/*
		System.out.println("-----------------Permutation Array-----");
		for(int i=0; i<permutArray.length; i++)
		{
			System.out.println(permutArray[i]);
		}
		System.out.println("-----------------------------------------");
		*/
		int eqOff = 1;

		boolean[] B = new boolean[L1.size()];
		for(int i=0; i < L1.size(); i++)
		{
			B[i] = false;
		}

		ArrayList<ArrayList<SortObject>> selfJoin_result = new ArrayList<ArrayList<SortObject>>();
		if(firstCondition.op.attrOperator == AttrOperator.aopGE || firstCondition.op.attrOperator == AttrOperator.aopLE
				&& secCondition.op.attrOperator == AttrOperator.aopGE || secCondition.op.attrOperator == AttrOperator.aopLE)
		{
			eqOff = 0;
		}

		//System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
		int pos = 0;
		int chunk = 4;
        int count = 0;
		int bloomSize = relSize/chunk;
		if(relSize%chunk > 0)
			bloomSize += 1;
		boolean[] bloomFilter = new boolean[bloomSize];
        //FileWriter writer = new FileWriter("QUERY_2B.txt");
		for(int i=0; i < L1.size(); i++)
		{
			pos = permutArray[i];
			B[pos] = true;
			bloomFilter[ pos/chunk ] = true;

			for(int k=pos/chunk; k<bloomSize; k++)
			{
				if(bloomFilter[k])
				{
					for(int j = k*chunk; j < min((k+1)*chunk, L1.size()); j++)
					{
						ArrayList<SortObject> al = new ArrayList<SortObject>();
						if(B[j] && j >= pos+eqOff)
						{
                            //writer.write("["+projectionFields.get(L1.get(j).rowIndex)+", "+projectionFields.get(L1.get(permutArray[i]).rowIndex)+"]\n");
							al.add(L1.get(j));
							al.add(L1.get(permutArray[i]));
							selfJoin_result.add(al);
                            //count++;
						}
					}
				}
			}
		}
        //writer.close();
//		System.out.println("--------B array------");
//		for(int i=0; i<relSize; i++)
//		{
//			System.out.println(B[i]);
//		}
//		System.out.println("-------------------------");

//		System.out.println("----------Result Array List--------------");
//
//		for(ArrayList<Integer> a : projectionFields)
//		{
//			for(int v:a)
//				System.out.print(v + " ");
//			System.out.println();
//		}



		// printing out the results of the self join
		//System.out.println("Result Tuple List Size "+count);
		for(int k=0; k<selfJoin_result.size(); k++)
		{
			ArrayList<SortObject> temp = selfJoin_result.get(k);
			System.out.print("[");
			printList(projectionFields.get(temp.get(0).rowIndex));
			System.out.print(", ");
			printList(projectionFields.get(temp.get(1).rowIndex));
			System.out.println("]");
		}
//		System.out.println("------------------------------------------");
	}

	public void printList(ArrayList<Integer> al)
	{
		for(int val:al)
		{
			System.out.print( val + " ");
		}
	}

	public int min(int param1, int param2)
	{
		return param1<param2? param1 : param2;
	}

	public ArrayList<ArrayList<Integer>> readValues(FileScan fs, FldSpec[] projFields, int col1, int col2)
			throws JoinsException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat, IOException
	{
		Tuple t = new Tuple();
		int index = 0;
		ArrayList<ArrayList<Integer>> listOfLists = new ArrayList<ArrayList<Integer>>();
		while(( t = fs.get_next()) != null)
		{
			SortObject sortObject = new SortObject();
			sortObject.setRowIndex(index);
			sortObject.setSortValue(t.getIntFld(col1));
			L1.add(sortObject);
			sortObject = new SortObject();
			sortObject.setRowIndex(index);
			sortObject.setSortValue(t.getIntFld(col2));
			L2.add(sortObject);
			ArrayList<Integer> al = new ArrayList<Integer>();
			ArrayList<Integer> uniqueProjFields = getUniqueProjectionFields(projFields);
			for(int i=0; i<uniqueProjFields.size(); i++)
			{
				al.add(t.getIntFld(uniqueProjFields.get(i)));
			}
			listOfLists.add(al);
			index++;
		}
		return listOfLists;
	}

	private ArrayList<Integer> getUniqueProjectionFields(FldSpec[] projFields)
	{
		ArrayList<Integer> uniqueProjFlds = new ArrayList<Integer>();
		for(int i=0; i<projFields.length; i++)
		{
			if(!uniqueProjFlds.contains(projFields[i].offset))
				uniqueProjFlds.add(projFields[i].offset);
		}
		return uniqueProjFlds;
	}

	public void sortArrayList(ArrayList<SortObject> al,  final TupleOrder order)
	{
		Collections.sort(al, new Comparator<SortObject>() {

			public int compare(SortObject o1, SortObject o2) {

				if (order.tupleOrder == TupleOrder.Ascending) {
					return o1.getSortValue() < o2.getSortValue() ? -1
							: o1.getSortValue() > o2.getSortValue() ? 1
							: 0;
				} else {
					return o1.getSortValue() > o2.getSortValue() ? -1
							: o1.getSortValue() < o2.getSortValue() ? 1
							: 0;
				}


			}
		});
	}
}