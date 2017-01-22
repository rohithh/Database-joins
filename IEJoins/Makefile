JDKPATH = /usr/lib/jvm/jdk1.8.0_71
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test IEJoins

StartHere:StartHere.java
	$(JAVAC) StartHere.java readRelations.java storeRelationInHeapFile.java queryUtils.java queryCondExpr.java IESelfJoin.java IESelfJoinNoHeapFile.java IESelfJoinSingle.java MemorySort.java SortObject.java IEJoins.java

iejoin: StartHere
	$(JAVA) IEJoins.StartHere

#Rohith's IEJoins

RJoins:RJoins.java
	$(JAVAC) RJoins.java

rjoins: RJoins
	$(JAVA) IEJoins.RJoins

clean:
	\rm -f $(CLASSPATH)/*.class *~ \#* core $(JARFILES) TRACE


