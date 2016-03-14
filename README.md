----------
-- UDF
----------
CREATE TEMPORARY FUNCTION sum AS 'oneonebang.hive.ql.udf.sum' ;
CREATE TEMPORARY FUNCTION tsum AS 'oneonebang.hive.ql.udf.tsum' ;
-- SELECT SUM(1,2,3) => 6
-- SELECT TSUM(array(1,2,3)) => 6

CREATE TEMPORARY FUNCTION json AS 'oneonebang.hive.ql.udf.json' ;
-- SELECT JSON(array(MAP(1,11,2,22), MAP(3,33,4,44))) => [{"1":11,"2":22},{"3":33,"4":44}]

CREATE TEMPORARY FUNCTION conj AS 'oneonebang.hive.ql.udf.conj' ;
-- SELECT CONJ(array(1,2,3),6) => [6,1,2,3]

CREATE TEMPORARY FUNCTION xconcat AS 'oneonebang.hive.ql.udf.concat' ;
CREATE TEMPORARY FUNCTION tconcat AS 'oneonebang.hive.ql.udf.tconcat' ;
-- SELECT XCONCAT(array("A", "B"), array("B", "C")) =>  ["A","B","B","C"]
-- SELECT TCONCAT(array( array("A", "B"), array("B", "C") )) => ["A","B","B","C"]

CREATE TEMPORARY FUNCTION str AS 'oneonebang.hive.ql.udf.str' ;
-- SELECT STR('array:', array(1,2,3), '-CURRENT_DATE:', CURRENT_DATE) => array:[1, 2, 3]-CURRENT_DATE:2016-03-14

CREATE TEMPORARY FUNCTION difference AS 'oneonebang.hive.ql.udf.difference' ;
-- SELECT DIFFERENCE(array(1,2,3,4), array(2,4, 5)) => [1,3]

CREATE TEMPORARY FUNCTION merge_with AS 'oneonebang.hive.ql.udf.merge_with' ;
-- SELECT MERGE_WITH('+', MAP(1,11,2,22), MAP(2,11,3,44), MAP(4,11)) => {1:11,2:33,3:44,4:11}

CREATE TEMPORARY FUNCTION filter AS 'oneonebang.hive.ql.udf.filter' ;
-- SELECT FILTER('#(<= 3 % 4)', array(1,2,3,4,5,6)) => [3,4]

CREATE TEMPORARY FUNCTION xmap AS 'oneonebang.hive.ql.udf.map' ;
-- SELECT XMAP('#(+ % 1)', array(1,2,3)) => [2,3,4]

CREATE TEMPORARY FUNCTION frequencies AS 'oneonebang.hive.ql.udf.frequencies' ;
-- SELECT FREQUENCIES( array(1,2,3,4,3,2) ) => {1:1,2:2,3:2,4:1}

CREATE TEMPORARY FUNCTION xdistinct AS 'oneonebang.hive.ql.udf.distinct' ;
-- SELECT XDISTINCT( array(1,2,3,4,3,2) ) => [1,2,3,4]

CREATE TEMPORARY FUNCTION xcount AS 'oneonebang.hive.ql.udf.count' ;
-- SELECT XCOUNT(array(1,2,3,4,6)) => 5
-- SELECT XCOUNT("ABCDAAA") =>  7

----------
-- UDTF
----------
CREATE TEMPORARY FUNCTION explode_json AS 'oneonebang.hive.ql.udtf.explode_json' ;

----------
-- UDAF
----------
CREATE TEMPORARY FUNCTION xcollect AS 'oneonebang.hive.ql.udaf.collect' ;
-- SELECT XCOLLECT(val) FROM ( SELECT STACK(3, ARRAY("a", "aa"), array("b", "bb"), array("c", "cc")) AS val ) tmp
-- => [["a","aa"],["b","bb"],["c","cc"]]

CREATE TEMPORARY FUNCTION gmerge_with AS 'oneonebang.hive.ql.udaf.merge_with' ;
-- SELECT GMERGE_WITH('+', val) FROM ( SELECT STACK(3, MAP("a",1), MAP("b",1), MAP("c", 1, "b", 1)) AS val ) tmp
-- => {"a":1,"b":2,"c":1}

CREATE TEMPORARY FUNCTION gsum AS 'oneonebang.hive.ql.udaf.sum' ;
-- SELECT GSUM(val) FROM ( SELECT STACK(3, 5, 6, 7) AS val) tmp
-- => 18

CREATE TEMPORARY FUNCTION gcount AS 'oneonebang.hive.ql.udaf.count' ;
-- SELECT GCOUNT(val) FROM ( SELECT STACK(3, 5, 6, 7) AS val) tmp
-- => 3
