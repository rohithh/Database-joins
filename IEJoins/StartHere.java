package IEJoins;

import global.SystemDefs;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import iterator.*;
import heap.*;

import java.lang.*;

/**
 * Created by varun on 3/14/16.
 */
public class StartHere {
    private static boolean OK = true;
    private static boolean FAIL = false;

    //File path to get the query and relations; Can hardcode to point to a different location if needed
    public static final String filePath = System.getProperty("user.dir").replace("/IEJoins", "/RelationNQuery");

    /**
     * Main method
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception, Throwable{

        int cont = 0;

        //----------Initializing the database----------
        initDB();

        do {
            System.gc();
            //----------Scan the file name of the query----------
            Scanner scanner = new Scanner(System.in);
            System.out.println("Please enter the name of the query file (.txt)");
            String name = scanner.next();

            //----------Read query file----------
            queryUtils qu = new queryUtils();
            qu.readQueryFile(name);

            //----------Print query----------
            System.out.println("\n"+qu.printQuery());

            //----------Join operation----------
            Runtime runtime = Runtime.getRuntime();
            long usedMemoryBefore = (runtime.totalMemory() - runtime.freeMemory())/1000000;
            joinOperation(qu);

            long usedMemoryAfter = (runtime.totalMemory() - runtime.freeMemory())/1000000;
            System.out.println("Memory used (MB): " + (usedMemoryAfter-usedMemoryBefore));
            System.gc();
            System.out.println("\nEnter 1 to select another query file or Enter 0 to exit");
            cont = scanner.nextInt();
        }while (cont==1);

        cleanUp();
        System.out.println("Exiting...");
    }

    public static void joinOperation(queryUtils qu) throws Throwable{
        System.out.println();
        boolean status= OK;
        int numPredicates = qu.getNumberOfPredicates();
        Scanner scanner = new Scanner(System.in);
        int choice = 0;

        readRelations rr = new readRelations();
        readRelations rr2 = new readRelations();

        if(qu.getNumberOfRelations()==1) {
            if(numPredicates==1) {
                System.out.println("Given query has one relation and single predicate");
            }
            else{
                System.out.println("Given query has one relation and two predicates");
            }
            System.out.println("\nPlease select the join operation to be performed: ");
            System.out.println("1. Nested Loop Join");
            System.out.println("2. IE SelfJoin");
            System.out.println("3. IE SelfJoin with optimizations");
            System.out.println("4. IE SelfJoin with optimizations, without writing relation to heapfile");
            choice = scanner.nextInt();
            System.out.println();

            rr2 =rr;
        }
        else if(qu.getNumberOfRelations()==2){
            if(numPredicates==1) {
                System.out.println("Given query has two relations and single predicate");
            }
            else{
                System.out.println("Given query has two relations and two predicates");
            }
            System.out.println("\nPlease select the join operation to be performed: ");
            System.out.println("1. Nested Loop Join");
            System.out.println("2. IE Join");
            System.out.println("3. IE Join with optimizations");
            choice = scanner.nextInt()+4;
            System.out.println();

        }


        queryCondExpr.setQueryUtils(qu);

        String nameOuter = qu.getOuterRelation();
        String nameInner = qu.getInnerRelation();
        boolean optimization =false;

        switch (choice){
            //--------------------------------------NESTED LOOP JOIN--------------------------------------
            case 1:
            case 5:
            {
                System.out.println("\nStoring Relations in Heap Files...\n");
                storeRelationInHeapFile sheap = new storeRelationInHeapFile();
                sheap.relationtoHeapFile(rr.readRelationsFile(qu.getOuterRelation() + ".txt"), qu.getOuterRelation());
                sheap.relationtoHeapFile(rr2.readRelationsFile(qu.getInnerRelation()+".txt"), qu.getInnerRelation());

                int numOuterColumns =rr.getAttributeTypes().size();
                int numInnerColumns =rr2.getAttributeTypes().size();
                //Starting File Scan for the outer relation
                long startTime = System.nanoTime();
                FileScan am = null;
                try {
                    am  = new FileScan(nameOuter+".in", queryCondExpr.getAttrType(rr.getAttributeTypes(), numOuterColumns),
                            queryCondExpr.getStrSizes(rr.getNumOfStringAtrr()),
                            (short)numOuterColumns, (short)numOuterColumns,
                            queryCondExpr.getOuterProjection(numOuterColumns), null);
                }
                catch (Exception e) {
                    status = FAIL;
                    System.err.println (""+e);
                    e.printStackTrace();
                }
                Tuple t = new Tuple();
                NestedLoopsJoins nlj = null;
                try {
                    nlj = new NestedLoopsJoins (queryCondExpr.getAttrType(rr.getAttributeTypes(),numOuterColumns),
                            numOuterColumns,
                            queryCondExpr.getStrSizes(rr.getNumOfStringAtrr()),
                            queryCondExpr.getAttrType(rr2.getAttributeTypes(),numInnerColumns), numInnerColumns,
                            queryCondExpr.getStrSizes(rr2.getNumOfStringAtrr()),
                            10000,
                            am, nameInner+".in",
                            queryCondExpr.getConditionExpr(), null, queryCondExpr.getProjection(),
                            queryCondExpr.getNumJoinedColumns());
                }
                catch (Exception e) {
                    System.err.println ("*** Error preparing for nested_loop_join");
                    System.err.println (""+e);
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
                }

                try {
                    //List<String> res = new ArrayList<String>();
                    while ((t =nlj.get_next()) !=null) {

                        t.print(queryCondExpr.getJoinedAttrType());
                    }
                    //System.out.println(res.size());
                    //writeOutputToFile(res,"QUERY_1B.txt");
                }catch (Exception e) {
                    System.err.println ("*** Error preparing for get_next tuple");
                    System.err.println (""+e);
                    Runtime.getRuntime().exit(1);
                }
                long endTime = System.nanoTime();
                System.out.println("\nTime to taken to complete Nested Loop Join: (ms) "+(endTime-startTime)/1000000);
            }break;

            //--------------IE SELF JOIN, SINGLE & DOUBLE PREDICATION, WITH & WITHOUT OPTIMIZATION--------------------------------
            case 3:
            optimization = true;
            case 2:
            {
                //TODO IE self join optimization?

                System.out.println("\nStoring Relations in Heap Files...\n");
                storeRelationInHeapFile sheap = new storeRelationInHeapFile();
                sheap.relationtoHeapFile(rr.readRelationsFile(qu.getOuterRelation() + ".txt"), qu.getOuterRelation());
                sheap.relationtoHeapFile(rr2.readRelationsFile(qu.getInnerRelation()+".txt"), qu.getInnerRelation());

                long startTime = System.nanoTime();
                IESelfJoin ieselfj = null;
                int numOuterColumns =rr.getAttributeTypes().size();

                try {
                    if(numPredicates == 2)
                    {
                        ieselfj = new IESelfJoin(qu,rr);
                        if(optimization){
                            ieselfj.optimizedSelfJoin();
                        }
                        else {
                            ieselfj.selfJoin();
                        }
                        //TODO writeOutputToFile(res,"QUERY_2B.txt");
                    }else
                    {
                        IESelfJoinSingle ies = new IESelfJoinSingle();
                        FileScan am = new FileScan(nameOuter + ".in", queryCondExpr.getAttrType(rr.getAttributeTypes(), numOuterColumns),
                                queryCondExpr.getStrSizes(rr.getNumOfStringAtrr()), (short) numOuterColumns, (short) numOuterColumns,
                                queryCondExpr.getOuterProjection(numOuterColumns), null);
                         //writeOutputToFile(ies.ieSelfJoin(am, optimization), "QUERY_2A.txt");
                        ies.ieSelfJoin(am, optimization);
                        //TODO writeOutputToFile(res,"QUERY_2A.txt");
                    }
                }
                catch (Exception e) {
                    System.err.println ("*** Error preparing for IE SelfJoin");
                    System.err.println (""+e);
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
                }
                long endTime = System.nanoTime();
                System.out.println("\nTime to taken to complete IE Self Join: (ms) " + (endTime - startTime) / 1000000);

            }break;

            //--------------------------IE SELF JOIN OPTIMIZATION, NO HEAPFILE WRITING & READING-----------------------------------
            case 4: {optimization = true;
                if(numPredicates==1){
                    System.out.println("Single predicate not supported");break;}

                long startTime = System.nanoTime();

                if(qu.getNumberOfPredicates()==2) {
                    IESelfJoinNoHeapFile ies = new IESelfJoinNoHeapFile();
                    try {
                        ies.ieSelfJoin(rr.readRelationsFile(qu.getOuterRelation() + ".txt"), optimization);
                        //writeOutputToFile(ies.ieSelfJoin(rr.readRelationsFile(qu.getOuterRelation() + ".txt"), optimization),"QUERY_2B.txt");
                    } catch (Exception e) {
                        System.err.println("*** Error preparing for IESelfJoinNoHeapFile");
                        System.err.println("" + e);
                        Runtime.getRuntime().exit(1);
                    }
                    long endTime = System.nanoTime();
                    System.out.println("\nTime to taken to complete IE Self Join: (ms) " + (endTime - startTime) / 1000000);
                }

            }break;

            //-------------------------------IE JOIN, WITH & WITHOUT OPTIMIZATION--------------------------------------
            case 7:
                optimization = true;
            case 6:{
                if(numPredicates==1){
                    System.out.println("Single predicate not supported");break;}

                System.out.println("\nStoring Relations in Heap Files...\n");
                storeRelationInHeapFile sheap = new storeRelationInHeapFile();
                sheap.relationtoHeapFile(rr.readRelationsFile(qu.getOuterRelation() + ".txt"), qu.getOuterRelation());
                sheap.relationtoHeapFile(rr2.readRelationsFile(qu.getInnerRelation()+".txt"), qu.getInnerRelation());

                long startTime = System.nanoTime();
                IEJoins iej = null;
                try {
                    iej = new IEJoins(qu,rr,rr2);

                    if(optimization){

                        iej.joinWithOptimizedQuery();
                    }
                    else{
                        writeOutputToFile(iej.joinWithQuery(),"QUERY_2C.txt");
                        //iej.joinWithQuery();
                    }

                }
                catch (Exception e) {
                    System.err.println ("*** Error preparing for IEJoin");
                    System.err.println (""+e);
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
                }
                long endTime = System.nanoTime();
                System.out.println("\nTime to taken to complete IE Join: (ms) " + (endTime - startTime) / 1000000);

            }break;
            default: {
                System.out.println("Incorrect choice");
            }break;
        }

    }

    public static void writeOutputToFile(List<String> res, String name) throws IOException{
        FileWriter writer = new FileWriter(name);
        res.add(0, "Number of joined tuples are " +Integer.toString(res.size())+"\n");
        for(String str: res) {
            writer.write(str);
        }
        writer.close();
    }
    public static void initDB(){
        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb";
        String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case I
        //I missed something
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }

        SystemDefs sysdef = new SystemDefs( dbpath, 100000, 5000, "Clock" );

    }
    public static void cleanUp(){
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb";
        String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;
        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }
    }
}
