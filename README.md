# Test demonstrating poor performance of IN queries under certain conditions 

We have a big collection which we like to keep indexed in-memory. We encountered
some massive performance degradation when using IN queries. As soon as some
boundary are hit the search takes a very long time.
 
There are test cases which should demonstrate the behavior. There is a test
for equal/in/or and tests for the jump in search speed.
 
We found a solution to speed up a slow IN query by replacing it with an OR
query with multiple sub-queries. This approach performs quite well. But things
can get messy as soon as there are multiple IN queries which must be replaced
(combinatorial explosion).
 
Is there  something else we could do but are not aware of to improve the search
speed? Wrong, missing indices? We are glad for any pointers.

# How to run?
You need at least Java 6 and Maven 3 installed. To run the tests execute the
following command in the root folder of this project. 

``` mvn test ```
   
