# stypes-flink

It is possible to evaluate [**STypeS NDL rewriting**](https://github.com/srapisarda/stypes) using Apache Flink.
It is easy to write an Apache Flink program. If, for example, we are taking into consideration the following NDL script:
 ```
p1(x,y) :− s(z,y), s(x,y).
p1(x,y) :− s(x,y), r(y,z).
```

the Apache Flink program can be something like below:
```scala
object FlinkRewriting {
  private def stringMapper: (String) => (String, String) = (p: String) => { 
    val line = p.split(',')
    (line.head, line.last)
  }
  // Apache Flink environment 
  val env = ExecutionEnvironment.getExecutionEnvironment
  // r and s relation definition from a CSV file resources stored in hadoop
  val r =env.readTextFile("hdfs:///user/hduser/data/r.csv").map(stringMapper) 
  val s = env.readTextFile("hdfs:///user/hduser/data/r.csv").map(stringMapper)
  // (1) - p1(x,y) :− s(z,y), s(x,y).
  val p1_1 = s.join(s).where(1).equalTo(1).map(term => (term. 2._1, term. 1._2)) 
  // (2) - p1(x,y) :− s(x,y), r(y,z).
  val p1_2 = s.join(r).where(1).equalTo(0).map(term => (term. 1._1, term. 2._1)) 
  // Union on conjunctive query (1) V (2) 
  val p1 = p1_1.union(p1_2) 
  // this is the evaluation of a distinct count action
  p1.distinct().count()
 }
```

How we can note from the listing above it that the program it is compact and more clear. 
The **stringMapper** function, transforms a string in a tuple **(t1,t2)** where **t1** and **t2** are both string. 
After, it is created an Apache Flink execution environment. 
The variable **r** and **s** are respectably created two DataSet[String, String] from a CSV file resources stored in hadoop.
Later on it is created the fist join operation. 
In Flink the join from two tuples **tp_x = (t1, . . . , tn)** and **tp_y(t1,...,tm)** is a new tuple **tp_x_y = (tp1,tp2)**. 
If for example **tp_1 = (t1,t2)** and **tp_2 = (t1,t2)** Flink creates a **tp_n = [(t1, t2), (t1, t2)]**. 
The function **where** is applied to the relation on **left-hand side LHS**, whereas the function **equalTo** is used for the **right-hand side RHS** relation. 
The index of the terms present in the **LHS** and **RHS** relations are starter form **0** (zero) to **n = t_n − 1**, where **t_n** is the total number of terms. 
If for instance we where doing a different join **p(x, y) :- w(x, y, z), v(z, x, y)** the join operation will be
```scala
val p = w.join(v).where(0,1,2).equalTo(1,2,0).map(t => (t. 1. 1, t. 2. 3)
```
In Apache Flink it is possible to decide which is the degree of parallelization that we want to use during the execution of the script. 
It is important to know that the level of parallelism cannot be bigger of the total number of **vcores** the cluster contains. 
Moreover, it is not possible to use all the cluster **vcores** because some of them can be busy in executing other tasks, 
for example, some can be dedicated to Hadoop administration.
