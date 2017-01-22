package IEJoins;

import global.AttrOperator;
import global.AttrType;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

import java.util.List;
import java.lang.*;


public class queryCondExpr {
    static queryUtils qu;

    public static void setQueryUtils(queryUtils qus){
        qu = qus;
    }


    public int[] queryNum(List<List<String>> listQ, List<List<String>> relation){
        int num = qu.getNumberOfPredicates();

        int[] result = new int[num*6];          //Operator, type1, type2, outer, operand1, inner, operand2

        switch (Integer.parseInt(listQ.get(2).get(1))){
            case queryUtils.quLT:
                result[0]= 1;
            case queryUtils.quLE:
                result[0]= 4;
            case queryUtils.quGT:
                result[0]= 2;
            case queryUtils.quGE:
                result[0]= 5;
        }
        result[1]=1;
        result[2]=1;
        result[3]=0;
        result[4]=3;
        result[5]=1;
        result[6]=3;

        return result;
    }

    //----------From Relation File----------
    public static FldSpec[] getOuterProjection(int numColums){
        FldSpec[] Rprojection = new FldSpec[numColums];
        for(int i =0; i<numColums;i++) {
            Rprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
        }
        return Rprojection;
    }
    public static FldSpec[] getInnerProjection(int numColums){
        FldSpec[] Rprojection = new FldSpec[numColums];
        for(int i =0; i<numColums;i++) {
            Rprojection[i] = new FldSpec(new RelSpec(RelSpec.innerRel), i+1);
        }
        return Rprojection;
    }


    public static AttrType [] getAttrType(List<String>insideArr, int numColums){
        AttrType [] Rtypes = new AttrType[numColums];
        for(int i = 0; i < insideArr.size(); i++) {
            if (insideArr.get(i).compareTo("attrInteger") == 0)
                Rtypes[i] = new AttrType(AttrType.attrInteger);
            else if (insideArr.get(i).compareTo("attrString") == 0)
                Rtypes[i] = new AttrType(AttrType.attrString);
            else if (insideArr.get(i).compareTo("attrReal") == 0)
                Rtypes[i] = new AttrType(AttrType.attrReal);
        }

        return Rtypes;
    }

    public static short[] getStrSizes(int numOfStrAttr){
        short  []  Rsizes = new short[numOfStrAttr] ;
        for(int i= 0; i<numOfStrAttr;i++)
            Rsizes[i] = 20;

        return Rsizes;
    }


    //----------From Query File----------
    public static CondExpr[] getConditionExpr(){
        List<String> joinCondition = qu.listOListsQ.get(2);
        //Condition Expression
        int nu = 2;
        if(qu.getNumberOfPredicates()==2){
            nu =3;
        }
        CondExpr[] expr  = new CondExpr[nu];
        expr[0] = new CondExpr();
        expr[1] = new CondExpr();


            /*expr[0].next = null;
            expr[0].op = new AttrOperator(AttrOperator.aopEQ);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
            expr[0].type2 = new AttrType(AttrType.attrSymbol);
            expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 3);

            expr[1] = null;*/


            expr[0].next = null;
            String numStr;
            int num = -1;
            //	Checking for first element Eg. R

            if(joinCondition.get(0).startsWith(qu.getOuterRelation()) ){
                //print("TEST");exit();
                expr[0].type1 = new AttrType(AttrType.attrSymbol);
                numStr = joinCondition.get(0).replaceAll("\\D+", "");
                num = Integer.parseInt(numStr);
                expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),num);
            }


            //	Checking for the second element which must be a relational operator

            try{
                num = Integer.parseInt( joinCondition.get(1));
            }
            catch(Exception e){
                System.err.println("Number expected : to signify condition expression");
                System.exit(0);
            }


            switch(num){
                case 0 : 	    expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
                    break;
                case 1 : 	    expr[0].op    = new AttrOperator(AttrOperator.aopLT);
                    break;
                case 4 : 	    expr[0].op    = new AttrOperator(AttrOperator.aopGT);
                    break;
                case 2 : 	    expr[0].op    = new AttrOperator(AttrOperator.aopLE);
                    break;
                case 3 : 	    expr[0].op    = new AttrOperator(AttrOperator.aopGE);
                    break;
                default : System.err.println("Invalid choice of number input");
                    System.out.println();
            }

            //	Next checking for second operand. Eg : S

            if(joinCondition.get(2).startsWith(qu.getInnerRelation())){
                expr[0].type2 = new AttrType(AttrType.attrSymbol);
                numStr = joinCondition.get(2).replaceAll("\\D+", "");
                num = Integer.parseInt(numStr);
                expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),num);
            }
            else{
                try {
                    int op1 = Integer.parseInt(joinCondition.get(2));
                    expr[0].type2 = new AttrType(AttrType.attrInteger);
                    expr[0].operand2.integer = op1;

                } catch (NumberFormatException e) {
                    System.out.println("Invalid operand");
                }
            }

        if(qu.getNumberOfPredicates()==2) {

            List<String> joinConditions = qu.listOListsQ.get(4);
            expr[2] = new CondExpr();
            expr[1].next = null;
            expr[2] = null;


            int nums = -1;
            //	Checking for first element Eg. R

            if(joinConditions.get(0).startsWith(qu.getOuterRelation()) ){
                //print("TEST");exit();
                expr[1].type1 = new AttrType(AttrType.attrSymbol);
                numStr = joinConditions.get(0).replaceAll("\\D+", "");
                nums = Integer.parseInt(numStr);
                expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),nums);
            }


            //	Checking for the second element which must be a relational operator

            try{
                num = Integer.parseInt( joinConditions.get(1));
            }
            catch(Exception e){
                System.err.println("Number expected : to signify condition expression");
                System.exit(0);
            }


            switch(num){
                case 0 : 	    expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
                    break;
                case 1 : 	    expr[1].op    = new AttrOperator(AttrOperator.aopLT);
                    break;
                case 4 : 	    expr[1].op    = new AttrOperator(AttrOperator.aopGT);
                    break;
                case 2 : 	    expr[1].op    = new AttrOperator(AttrOperator.aopLE);
                    break;
                case 3 : 	    expr[1].op    = new AttrOperator(AttrOperator.aopGE);
                    break;
                default : System.err.println("Invalid choice of number input");
                    System.out.println();
            }

            //	Next checking for second operand. Eg : S

            if(joinConditions.get(2).startsWith(qu.getInnerRelation())){
                expr[1].type2 = new AttrType(AttrType.attrSymbol);
                numStr = joinConditions.get(2).replaceAll("\\D+", "");
                nums = Integer.parseInt(numStr);
                expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),nums);
            }
            else{
                try {
                    int op1 = Integer.parseInt(joinConditions.get(2));
                    expr[1].type2 = new AttrType(AttrType.attrInteger);
                    expr[1].operand2.integer = op1;

                } catch (NumberFormatException e) {
                    System.out.println("Invalid operand");
                }
            }

        }
        else{
            expr[1] = null;
        }

        return expr;
    }


    public static FldSpec [] getProjection(){
        List<String> joinCondition = qu.listOListsQ.get(0);
        boolean flag =false;
        FldSpec []  proj1 = new FldSpec[qu.listOListsQ.get(0).size()];
        for(int i =0; i<joinCondition.size();i++) {
            if (joinCondition.get(i).startsWith(qu.getOuterRelation()) && !flag) {
                proj1[i] = new FldSpec(new RelSpec(RelSpec.outer), Integer.parseInt(joinCondition.get(i).replaceAll("\\D+", "")));
                flag = true;
            }
            else if(joinCondition.get(i).startsWith(qu.getInnerRelation())){
                proj1[i]=new FldSpec(new RelSpec(RelSpec.innerRel), Integer.parseInt(joinCondition.get(i).replaceAll("\\D+", "")));
            }
        }
        return proj1;
    }


    public static AttrType[] getJoinedAttrType(){
        //----------Joined Tuple
        AttrType[] Jtypes = new AttrType[qu.listOListsQ.get(0).size()];

        for(int i = 0; i < qu.listOListsQ.get(0).size(); i++) {
            //System.out.println(insideArr.get(i).replaceAll("[^A-Za-z]+", ""));
            //if (insideArr.get(i).compareTo("attrInteger") == 0)
                Jtypes[i] = new AttrType(AttrType.attrInteger);
            /*else if (insideArr.get(i).compareTo("attrString") == 0)
                Jtypes[i] = new AttrType(AttrType.attrString);
            else if (insideArr.get(i).compareTo("attrReal") == 0)
                Jtypes[i] = new AttrType(AttrType.attrReal);*/
        }

        //short  []  Jsizes = new short[1];
        //Jsizes[0] = 30;
        return Jtypes;

    }


    public static int getNumJoinedColumns(){
        return qu.listOListsQ.get(0).size();
    }


}
