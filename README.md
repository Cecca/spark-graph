spark-graph
===========

Graph algorithms implemented in Spark: ball decomposition and diameter computation

Branches
--------

 - master: [![Build Status](https://travis-ci.org/Cecca/spark-graph.png?branch=master)](https://travis-ci.org/Cecca/spark-graph)
 - small-word-array: [![Build Status](https://travis-ci.org/Cecca/spark-graph.png?branch=small-word-array)](https://travis-ci.org/Cecca/spark-graph)
                     Optimization of hyper-anf that uses tightly packed arrays of words of 32 bits.
 - cogroup: [![Build Status](https://travis-ci.org/Cecca/spark-graph.png?branch=cogroup)](https://travis-ci.org/Cecca/spark-graph)
            Optimization of all graph algorithms that uses the `cogroup` primitive instead of the more costly `join`.

Changelog
---------

 - v0.2.0: Switch to Spark 0.8.0-incubating
 - v0.1.0: Implement ball decomposition (deterministic and randomized)
           and the distributed version of HyperANF
