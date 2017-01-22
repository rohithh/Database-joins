package IEJoins;
import java.util.List;
import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;

/**
 * Created by varun on 3/24/16.
 */
public class IESelfJoinSingle {

    public List<String> ieSelfJoin(FileScan am, boolean optimize) throws IOException,heap.FieldNumberOutOfBoundException  {
        List<String> res = new ArrayList<String>();

        CondExpr[] condExprArray = queryCondExpr.getConditionExpr();
        CondExpr firstCondition = condExprArray[0];
        int outTupleColumn = queryCondExpr.getProjection()[0].offset;
        final int columnNumber = firstCondition.operand1.symbol.offset;
        //int rows = am.size();

        List<PTuple> L1 = new ArrayList<PTuple>();
        List<PTuple> JL = new ArrayList<PTuple>();

        Tuple t = new Tuple();
        int index = 0;
        try {
            while ((t = am.get_next()) != null) {
                L1.add( new PTuple(index, t.getIntFld(columnNumber)));
                JL.add( new PTuple(index, t.getIntFld(outTupleColumn)));
                index++;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error while scanning the file");
        }

     /*   for (int i = 1; i < rows; i++) {
            L1.add( new PTuple(i-1, Integer.parseInt(am.get(i).get(columnNumber-1))));
            JL.add( new PTuple(i-1, Integer.parseInt(am.get(i).get(outTupleColumn-1))));
        }*/

        // Sort L1 in ascending order
        Collections.sort(L1, new Comparator<PTuple>() {
            public int compare(PTuple o1, PTuple o2) {
                return o1.Value < o2.Value ? -1
                        : o1.Value > o2.Value ? 1
                        : 0;
            }
        });

        /*for(int i =0; i<L1.size();i++){
            System.out.println("L1 "+L1.get(i).Value);
        }*/
        if(!optimize || L1.size()<8) {
            boolean[] B = new boolean[L1.size()];
            int count = 0;
            int eqOff = 1;
            if (firstCondition.op.attrOperator == AttrOperator.aopGE || firstCondition.op.attrOperator == AttrOperator.aopLE) {
                eqOff = 0;
                System.out.println("check");
            }
            for (int i = L1.size() - 1; i >= 0; i--) {
            /*for(int k =0 ; k<B.length;k++)
                System.out.print(pos + " "+B[k] + " ");*/
                //System.out.println(i);
                B[i] = true;
                for (int j = i + eqOff; j < L1.size(); j++) {
                    if (B[j] && L1.get(i).Value != L1.get(j).Value) {
                        System.out.println("[" + JL.get(L1.get(j).rowIndex).Value + ", " + JL.get(L1.get(i).rowIndex).Value + "]");
                        //System.out.println("Check");
                        count++;
                    }
                }
            }
            //System.out.println(count);
        }
        else {
            int chunk = 4;
            boolean[] B = new boolean[L1.size()];
            boolean[] Bloom;
            Bloom = new boolean[L1.size() / chunk];
            long count = 0;
            FileWriter writer = new FileWriter("QUERY_2A.txt");
            int eqOff = 1;
            if (firstCondition.op.attrOperator == AttrOperator.aopGE || firstCondition.op.attrOperator == AttrOperator.aopLE) {
                eqOff = 0;
                System.out.println("check");
            }
            for (int i = L1.size() - 1; i >= 0; i--) {
            /*for(int k =0 ; k<B.length;k++)
                System.out.print(pos + " "+B[k] + " ");*/
                //System.out.println(i);
                Bloom[i / chunk] = true;
                B[i] = true;
                if(((i/chunk)+eqOff) <L1.size() && Bloom[(i/chunk)+eqOff-1]) {
                    for (int j = i + eqOff; j < L1.size(); j++) {
                        if (B[j] && L1.get(i).Value != L1.get(j).Value) {
                            System.out.println("[" + JL.get(L1.get(j).rowIndex).Value + ", " + JL.get(L1.get(i).rowIndex).Value + "]");
                            //res.add("[" + JL.get(L1.get(j).rowIndex).Value + ", " + JL.get(L1.get(i).rowIndex).Value + "]");
                            //writer.write("[" + JL.get(L1.get(j).rowIndex).Value + ", " + JL.get(L1.get(i).rowIndex).Value + "]\n");
                            //System.out.println("Check");
                            //TODO
                            count++;
                        }
                    }
                }
            }
            writer.close();
            //System.out.println(count);
        }
        return res;

    }
}
