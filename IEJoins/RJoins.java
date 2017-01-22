package IEJoins;

        import java.io.BufferedReader;
        import java.io.IOException;
        import java.io.InputStreamReader;
        import java.nio.file.Files;
        import java.nio.file.Paths;
        import java.util.ArrayList;
        import java.util.Vector;

        import iterator.*;
        import heap.*;
        import global.*;
        import index.*;
        import java.io.*;
        import java.util.*;
        import java.lang.*;
        import diskmgr.*;
        import bufmgr.*;
        import btree.*;
        import catalog.*;
        import global.AttrOperator;
        import global.AttrType;
        import global.RID;
        import global.SystemDefs;
        import global.TupleOrder;
        import iterator.CondExpr;
        import iterator.FileScan;
        import iterator.FldSpec;
        import iterator.NestedLoopsJoins;
        import iterator.RelSpec;
        import iterator.Sort;


/*
 * Few important points to be noted :
 * 1) p_i1 and p_i2 contain the sorted files that we are considering
 * 2) Query1_CondExpr is used to set the join condition [ invoked by passing outFilter param see Query1() ]
 * TODO First perform a FileScan() using all relevant arguments, result is "am" pointer that can iterate to produce all contents of the file
 * TODO Find how to insert the pages into the database by means of FileScan > get_file_entry()
 * TODO Try using addFileEntry() in DB.java to create a new file
 *
 */

public class RJoins {
    public static void main(String args[]) throws IOException {

	/*
	 * First take input of file names from user
	 */

        String fileNames[] = new String[3];
        System.out.println("Please enter the First file name, second file name and query file name in order without extension");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        fileNames[0] = "R1";
        fileNames[1] = "S1";
        fileNames[2] = "query_1a";

	/*
	 * Uncomment and utilize section while actually running program
	 */

        fileNames[0] = br.readLine();
        fileNames[1] = br.readLine();
        fileNames[2] = br.readLine();

	/*
	 * Section ends
	 */



        // First read from R.txt

        ArrayList<ArrayList> ArrayF1 = new ArrayList<ArrayList>();
        ArrayList<String> strs = new ArrayList<String>();

        for(String line : Files.readAllLines(Paths.get("/home/varun/MEGAsync/ASU/Spring2016/CSE510DatabaseManagementSystemImplementation/Project/Test/minjava/javaminibase/src/RelationNQuery/"+fileNames[0]+".txt"))){
            strs.add(line);
        }
        //System.out.println(strs.get(0));
        for(String x : strs){
            ArrayList<String> insideArray = new ArrayList<String>();
            for(String y : x.split(",")){
                //System.out.print(y+" | ");
                insideArray.add(y);
            }
            ArrayF1.add(insideArray);
            //System.out.println();
        }
        int i = 0;
        System.out.println("\n=================================\n");
        //ArrayList<ArrayList> iterArr = new ArrayList<ArrayList>();
        for( ArrayList<String> iterArr : ArrayF1){
            for(String x : iterArr){
                System.out.print(x + " | ");
                if(++i % 4 == 0){
                    System.out.println();;
                }
            }
        }

	/*
	 * Now read from S.txt
	 */

        ArrayList<ArrayList> ArrayF2 = new ArrayList<ArrayList>();
        ArrayList<String> strs_S = new ArrayList<String>();

        for(String line : Files.readAllLines(Paths.get("/home/varun/MEGAsync/ASU/Spring2016/CSE510DatabaseManagementSystemImplementation/Project/Test/minjava/javaminibase/src/RelationNQuery/"+fileNames[1]+".txt"))){
            strs_S.add(line);
        }
        //System.out.println(strs.get(0));
        for(String x : strs_S){
            ArrayList<String> insideArray = new ArrayList<String>();
            for(String y : x.split(",")){
                //System.out.print(y+" | ");
                insideArray.add(y);
            }
            ArrayF2.add(insideArray);
            //System.out.println();
        }
        i = 0;
        System.out.println("\n=================	S_Array	==================\n");
        //ArrayList<ArrayList> iterArr = new ArrayList<ArrayList>();
        for( ArrayList<String> iterArr : ArrayF2){
            for(String x : iterArr){
                System.out.print(x + " | ");
                if(++i % 4 == 0){
                    System.out.println();;
                }
            }
        }


	/*
	 * Now read query
	 */

        ArrayList<ArrayList> ArrayQuery = new ArrayList<ArrayList>();
        ArrayList<String> strs_Query = new ArrayList<String>();

        for(String line : Files.readAllLines(Paths.get("/home/varun/MEGAsync/ASU/Spring2016/CSE510DatabaseManagementSystemImplementation/Project/Test/minjava/javaminibase/src/RelationNQuery/"+fileNames[2]+".txt"))){
            strs_Query.add(line);
        }
        for(String x : strs_Query){
            ArrayList<String> insideArray = new ArrayList<String>();
            for(String y : x.split(" ")){
                //System.out.print(y+" | ");
                insideArray.add(y);
            }
            ArrayQuery.add(insideArray);
            //System.out.println();
        }
        i = 0;
        System.out.println("\n=================	Query	==================\n");
        //ArrayList<ArrayList> iterArr = new ArrayList<ArrayList>();
        for( ArrayList<String> iterArr : ArrayQuery){
            for(String x : iterArr){
                System.out.print(x + " ");
                if(++i % 2 == 0 ){
                    if(x.compareTo(iterArr.get(iterArr.size()-1)) == 0){
                        System.out.println();

                    }
                }
            }
        }
        System.out.println();

	/*
	 *  Now implementing the Nested Loop Joins
	 */
        boolean OK = true;
        boolean FAIL = false;
        boolean status = OK;
        CondExpr [] outFilter  = new CondExpr[3];
        outFilter[0] = new CondExpr();
        outFilter[1] = new CondExpr();
        outFilter[2] = new CondExpr();
        CondExpr [] outFilter2 = new CondExpr[3];
        outFilter2[0] = new CondExpr();
        outFilter2[1] = new CondExpr();
        outFilter2[2] = new CondExpr();

        // final boolean  OK = true;
        //    boolean status = OK;

        String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb";
        String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

        String remove_cmd = "/bin/rm -rf ";
        String remove_logcmd = remove_cmd + logpath;
        String remove_dbcmd = remove_cmd + dbpath;
        String remove_joincmd = remove_cmd + dbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
            Runtime.getRuntime().exec(remove_joincmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }


	    /*
	    ExtendedSystemDefs extSysDef =
	      new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
	      1000,500,200,"Clock");
	    */

        SystemDefs sysdef = new SystemDefs( dbpath, 1000, 50, "Clock" );

	    /*
	     * Creating heap file of File 1
	     */

        // creating the f1 relation
        AttrType [] Stypes = new AttrType[4];
        Stypes[0] = new AttrType (AttrType.attrInteger);
        Stypes[1] = new AttrType (AttrType.attrInteger);
        Stypes[2] = new AttrType (AttrType.attrInteger);
        Stypes[3] = new AttrType (AttrType.attrInteger);

        //SOS
        short [] Ssizes = new short [1];
        Ssizes[0] = 30; //first elt. is 30
        //	    final boolean FAIL = false;
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 4,Stypes, Ssizes);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();
        //  System.out.println("size="+size);System.exit(0);
        // inserting the tuple into file "sailors"
        RID             rid;
        Heapfile        f = null;
        try {
            f = new Heapfile("R.in");
        }
        catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 4, Stypes, Ssizes);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

	    /*
	     * My code to take input
	     *
	    try {
	RJoins.main(null);
	System.out.println("HEY");
	System.exit(0);
	} catch (IOException e1) {
	//  Auto-generated catch block
	e1.printStackTrace();
	}
	    /*
	     * My code ends
	     */
        i = 0;
        int flag = 0;
        for(ArrayList<String> arr : ArrayF1){
            i = 0;
            for(String str : arr){
                if(flag == 0){

                }
                else{
                    try {
                        t.setIntFld(++i, Integer.parseInt(str));
                    } catch (Exception e) {
                        //  Auto-generated catch block
                        e.printStackTrace();
                        exit();
                    }
                }
            }

            if(flag == 1){
                try {

                    rid = f.insertRecord(t.returnTupleByteArray());
                }
                catch (Exception e) {
                    System.err.println("*** error in Heapfile.insertRecord() ***");
                    status = FAIL;
                    e.printStackTrace();
                }
            }
            if(flag == 0){
                flag = 1;
            }
        }

        if (status != OK) {
            //bail out
            System.err.println ("*** Error creating relation for sailors");
            Runtime.getRuntime().exit(1);
        }

	    /*
	     *  creating the F-2 Heap file
	     */
        AttrType [] F2types = new AttrType[4];
        F2types[0] = new AttrType (AttrType.attrInteger);
        F2types[1] = new AttrType (AttrType.attrInteger);
        F2types[2] = new AttrType (AttrType.attrInteger);
        F2types[3] = new AttrType (AttrType.attrInteger);

        //SOS
        short [] F2sizes = new short [1];
        F2sizes[0] = 30; //first elt. is 30
        t = new Tuple();
        try {
            t.setHdr((short) 4,F2types, F2sizes);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

        size = t.size();
        //  System.out.println("size="+size);System.exit(0);
        // inserting the tuple into file "sailors"
        f = null;
        try {
            f = new Heapfile("S.in");
        }
        catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 4, F2types, F2sizes);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            status = FAIL;
            e.printStackTrace();
        }

	    /*
	     * My code to take input
	     *
	    try {
	RJoins.main(null);
	System.out.println("HEY");
	System.exit(0);
	} catch (IOException e1) {
	//  Auto-generated catch block
	e1.printStackTrace();
	}
	    /*
	     * My code ends
	     */
        i = 0;
        flag = 0;
        for(ArrayList<String> arr : ArrayF2){
            i = 0;
            for(String str : arr){
                if(flag == 0){

                }
                else{
                    try {
                        t.setIntFld(++i, Integer.parseInt(str));
                    } catch (Exception e) {
                        //  Auto-generated catch block
                        e.printStackTrace();
                        exit();
                    }
                }
            }

            if(flag == 1){
                try {

                    rid = f.insertRecord(t.returnTupleByteArray());
                }
                catch (Exception e) {
                    System.err.println("*** error in Heapfile.insertRecord() ***");
                    status = FAIL;
                    e.printStackTrace();
                }
            }
            if(flag == 0){
                flag = 1;
            }
        }
        //    exit();
        if (status != OK) {
            //bail out
            System.err.println ("*** Error creating relation for sailors");
            Runtime.getRuntime().exit(1);
        }


        t = new Tuple();
        t = null;

	      /*
	       * Next find out the fTypes1 and fTypes2 by scanning first lines of input files
	       */

        //	First construct for fTypes1

        AttrType [] fTypes1;
        AttrType [] fTypes2;

        short [] fTypes1Sizes = new short[1];
        fTypes1Sizes[0] = 30;

        ArrayList<String> insideArr = ArrayF1.get(0);
        String []  s = new String[insideArr.size()];

        fTypes1 = new AttrType[insideArr.size()];

        for(i = 0; i < insideArr.size(); i++){
            if(insideArr.get(i).compareTo("attrInteger") == 0)
                fTypes1[i] = 	new AttrType(AttrType.attrInteger);
            else if(insideArr.get(i).compareTo("attrString") == 0)
                fTypes1[i] = 	new AttrType(AttrType.attrString);
            else if(insideArr.get(i).compareTo("attrReal") == 0)
                fTypes1[i] = 	new AttrType(AttrType.attrReal);
            else
            {
                print(i);
                print(insideArr.get(i));
                error("Incorrect attribute type");

            }
        }

        //	@TODO Single predicate queries

        //	Now construct for fTypes2

        short [] fTypes2Sizes = new short[1];
        fTypes2Sizes[0] = 30;

        insideArr = ArrayF2.get(0);
        s = new String[insideArr.size()];

        fTypes2 = new AttrType[insideArr.size()];

        for(i = 0; i < insideArr.size(); i++){
            if(insideArr.get(i).compareTo("attrInteger") == 0)
                fTypes2[i] = 	new AttrType(AttrType.attrInteger);
            else if(insideArr.get(i).compareTo("attrString") == 0)
                fTypes2[i] = 	new AttrType(AttrType.attrString);
            else if(insideArr.get(i).compareTo("attrReal") == 0)
                fTypes2[i] = 	new AttrType(AttrType.attrReal);
            else
            {
                print(i);
                print(insideArr.get(i));
                error("Incorrect attribute type");

            }
        }

	      /*
	       * Segment ends
	       */

        Query_CondExpr(outFilter, outFilter2,ArrayQuery,fileNames,ArrayF1, ArrayF2);

	      /*
	       * File Scans begin here
	       */


        AttrType [] Jtypes = {
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrInteger),
        };

        short  []  Jsizes = new short[1];
        Jsizes[0] = 30;
        AttrType [] JJtype = {
                new AttrType(AttrType.attrInteger),
        };

        short [] JJsize = new short[1];
        JJsize[0] = 30;


        //@TODO Make the below proj dynamic

        FldSpec []  proj1 = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.innerRel), 1)
        }; // S.sname, R.bid

        FldSpec [] proj2  = {
                new FldSpec(new RelSpec(RelSpec.outer), 1)
        };

        FldSpec [] Rprojection = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2),
                new FldSpec(new RelSpec(RelSpec.outer), 3),
                new FldSpec(new RelSpec(RelSpec.outer), 4)
        };





        FileScan am = null;
        try {
            am  = new FileScan("R.in", fTypes1, fTypes1Sizes,
                    (short)4, (short)4,
                    Rprojection, null);
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }
	   /*   try {
	while((t = am.get_next()) != null){
	//	print(t.getIntFld(1));
	}
	} catch (Exception e1) {
	// TODO Auto-generated catch block
	e1.printStackTrace();
	}*/
        // exit();
        if (status != OK) {
            //bail out

            System.err.println ("*** Error setting up scan for sailors");
            Runtime.getRuntime().exit(1);
        }

        FldSpec [] Sprojection = {
                new FldSpec(new RelSpec(RelSpec.innerRel), 1),
                new FldSpec(new RelSpec(RelSpec.innerRel), 2),
                new FldSpec(new RelSpec(RelSpec.innerRel), 3),
                new FldSpec(new RelSpec(RelSpec.innerRel), 4)
        };


        NestedLoopsJoins inl = null;
        try {
            inl = new NestedLoopsJoins (fTypes1, 4, fTypes1Sizes,
                    fTypes2, 4, fTypes2Sizes,
                    10,
                    am, "S.in",
                    outFilter, null, proj1, 2);
        }
        catch (Exception e) {
            System.err.println ("*** Error preparing for nested_loop_join");
            System.err.println (""+e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        try {
            while((t = inl.get_next()) != null){
                //print(t.getIntFld(1)+"|");
                t.print(Jtypes);
            }
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        exit();
/*
	      TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
	      Sort sort_names = null;
	      try {
	sort_names = new Sort (JJtype,(short)1, JJsize,
	       (iterator.Iterator) inl, 1, ascending, JJsize[0], 10);
	      }
	      catch (Exception e) {
	System.err.println ("*** Error preparing for sorting");
	System.err.println (""+e);
	Runtime.getRuntime().exit(1);
	      }
	      try {
	    	while ((t =sort_names.get_next()) !=null) {
	    	  t.print(JJtype);
	    	}
	    	      }catch (Exception e) {
	    	System.err.println ("*** Error preparing for get_next tuple");
	    	System.err.println (""+e);
	    	Runtime.getRuntime().exit(1);
	    	      }*/

    }

    private static void print(String string) {

        System.out.println(string);
    }

    private static void print(int string) {

        System.out.println(string);
    }

    private static void success(String x) {
        System.out.println(x);
    }
    private static void exit() {
        System.exit(0);
    }

    private static void error(String string) {
        System.err.println(string);
        System.exit(0);
    }

    private static void Query_CondExpr(CondExpr[] expr, CondExpr[] expr2, ArrayList<ArrayList> query,String fileNames[], ArrayList<ArrayList> arrayF1, ArrayList<ArrayList> arrayF2) {
        // TODO Auto-generated method stub

	/*
	 * First I determine the contents and tuples inside the join condition of outsideArray
	 */

        //JoinsDriver(query,arrayF1,arrayF2);


        ArrayList<String> joinCondition = query.get(2);
        expr[0].next = null;
        String numStr;
        int num = -1;
        //	Checking for first element Eg. R_1

        if(joinCondition.get(0).startsWith(fileNames[0]) || joinCondition.get(0).startsWith(fileNames[1]) || joinCondition.get(0).startsWith("R") || joinCondition.get(0).startsWith("S")){
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
            //	case 3 : 	    expr[0].op    = new AttrOperator(AttrOperator.aopNE);
            //	break;
            case 2 : 	    expr[0].op    = new AttrOperator(AttrOperator.aopLE);
                break;
            case 3 : 	    expr[0].op    = new AttrOperator(AttrOperator.aopGE);
                break;
            //	case 6 : 	    expr[0].op    = new AttrOperator(AttrOperator.aopNOT);
            //	break;
            default : System.err.println("Invalid choice of number input");
                System.out.println();
        }

        //	Next checking for second operand. Eg : S_1

        if(joinCondition.get(2).startsWith(fileNames[0]) || joinCondition.get(2).startsWith(fileNames[1]) ||
                joinCondition.get(2).startsWith("R") || joinCondition.get(2).startsWith("S")){
            expr[0].type2 = new AttrType(AttrType.attrSymbol);
            numStr = joinCondition.get(0).replaceAll("\\D+", "");
            num = Integer.parseInt(numStr);
            expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),num);
        }

        expr[1] = null;

	/*
	 * TODO Add functionality for double predicates
	 *
	if(query.get(3).get(0) == "AND"){

	}


	/*
	 * Functionality for double predicates ends
	 */



    }

}