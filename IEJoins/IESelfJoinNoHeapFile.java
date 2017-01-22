package IEJoins;

import global.AttrOperator;
import iterator.CondExpr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by varun on 3/23/16.
 */

/**
 *  IE Self Join is for double predicates, It is an in memory join function
 */
public class IESelfJoinNoHeapFile {

    public List<PTuple> sortArrayList(List<PTuple> L, final boolean ascend)
    {
        Collections.sort(L, new Comparator<PTuple>() {
            public int compare(PTuple o1, PTuple o2) {
                if (ascend) {
                    return o1.Value < o2.Value ? -1
                            : o1.Value > o2.Value ? 1
                            : 0;
                } else {
                    return o1.Value < o2.Value ? 1
                            : o1.Value > o2.Value ? -1
                            : 0;
                }
            }
        });
        return L;
    }

    public List<String> ieSelfJoin(List<List<String>> am, boolean optimize) throws IOException,heap.FieldNumberOutOfBoundException  {

        List<String> res = new ArrayList<String>();

        CondExpr[] condExprArray = queryCondExpr.getConditionExpr();
        CondExpr firstCondition = condExprArray[0];
        CondExpr secondCondition = condExprArray[1];
        //Assuming in a predicate both have same column number
        final int columnNumber = firstCondition.operand1.symbol.offset;
        final int columnNumber2 = secondCondition.operand1.symbol.offset;
        int outTupleColumn = queryCondExpr.getProjection()[0].offset;
        int rows = am.size();

        List<PTuple> L1 = new ArrayList<PTuple>();
        List<PTuple> L2 = new ArrayList<PTuple>();
        List<PTuple> JL = new ArrayList<PTuple>();
        //int[] L1array = new int[numOfRows-1];
        //System.out.println(columnNumber+" "+columnNumber2+" "+outTupleColumn);
        for (int i = 1; i < rows; i++) {
            L1.add( new PTuple(i-1, Integer.parseInt(am.get(i).get(columnNumber-1))));
            L2.add( new PTuple(i-1,  Integer.parseInt(am.get(i).get(columnNumber2-1))));
            JL.add( new PTuple(i-1, Integer.parseInt(am.get(i).get(outTupleColumn-1))));
            }

        System.out.println("");

        if ( firstCondition.op.attrOperator == AttrOperator.aopGT || firstCondition.op.attrOperator == AttrOperator.aopGE ) {
            L1  = sortArrayList(L1, true);      // Sort L1 in Ascending order
        }
        else if (firstCondition.op.attrOperator == AttrOperator.aopLT || firstCondition.op.attrOperator == AttrOperator.aopLE) {
            L1  = sortArrayList(L1, false);
        }

        if (secondCondition.op.attrOperator == AttrOperator.aopGT || secondCondition.op.attrOperator == AttrOperator.aopGE) {
            L2 = sortArrayList(L2, false);      // Sort L2 descending order

        } else if (secondCondition.op.attrOperator == AttrOperator.aopLT || secondCondition.op.attrOperator == AttrOperator.aopLE) {
            L2 = sortArrayList(L2, true);       // Sort L2 in ascending order

        }

            /*for(int i =0; i<L1.size();i++){
                System.out.println("L1 "+L1.get(i).Value);
                System.out.println("L2 " +L2.get(i).Value);
            }*/

        //compute the permutation array P of L 2 w.r.t. L 1
        int[] P = new int[L1.size()];
        for (int i = 0; i < L2.size(); i++) {
            for (int j = 0; j < L1.size(); j++) {
                if (L2.get(i).rowIndex==(L1.get(j).rowIndex)) {
                    P[i] = j;
                    break;
                }
            }
        }


        if(!optimize) {
            boolean[] B = new boolean[L1.size()];
            int eqOff = 1;

            if (firstCondition.op.attrOperator == AttrOperator.aopGE || firstCondition.op.attrOperator == AttrOperator.aopLE
                    && secondCondition.op.attrOperator == AttrOperator.aopGE || secondCondition.op.attrOperator == AttrOperator.aopLE) {//
                eqOff = 0;
            }

            //----------IE Self Join----------
            for (int i = 0; i < L1.size(); i++) {
                int pos = P[i];
                B[pos] = true;
                for (int j = pos + eqOff; j < L1.size(); j++) {
                    if (B[j] && L1.get(i).Value!=L1.get(j).Value) {
                        System.out.println("[" + JL.get(L1.get(j).rowIndex).Value + ", " + JL.get(L1.get(P[i]).rowIndex).Value + "]");
                    }
                }
            }
        }
        else{
            int chunk = 4;
            boolean[] B = new boolean[L1.size()];
            boolean[] Bloom;
            Bloom = new boolean[L1.size() / chunk];
            int eqOff = 1;

            if (firstCondition.op.attrOperator == AttrOperator.aopGE || firstCondition.op.attrOperator == AttrOperator.aopLE
                    && secondCondition.op.attrOperator == AttrOperator.aopGE || secondCondition.op.attrOperator == AttrOperator.aopLE) {//
                eqOff = 0;
            }

            int count =0;

            //----------IE Self Join optimized with Bloom filter----------
            for (int i = 0; i < L1.size(); i++) {
                int pos = P[i];
                Bloom[pos / chunk] = true;
                B[pos] = true;
                if(((pos/chunk)+eqOff) <L1.size() && Bloom[(pos/chunk)+eqOff-1]) {
                    for (int j = pos + eqOff; j < L1.size(); j++) {
                        if (B[j] && L1.get(i).Value!=L1.get(j).Value) {
                            System.out.println("[" + JL.get(L1.get(j).rowIndex).Value + ", " + JL.get(L1.get(P[i]).rowIndex).Value + "]");
                            res.add("[" + JL.get(L1.get(j).rowIndex).Value + ", " + JL.get(L1.get(P[i]).rowIndex).Value + "]");
                            count++;
                        }
                    }
                }
            }
            //System.out.println(count);
        }
        return res;

    }

}
class PTuple {
    public int rowIndex;
    public int Value;
    public PTuple(int r, int v){
        rowIndex = r;
        Value = v;
    }
}
