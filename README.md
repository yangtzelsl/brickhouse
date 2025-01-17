# Hive UDF使用教程
```bash
 # 上传jar包
 hdfs dfs -put -f brickhouse-0.7.1-SNAPSHOT.jar /udf
 
 # 关注一下用户和组，是否具有权限
 
 # 创建临时函数(也可创建永久函数)
# 函数说明：使用驼峰命名，类的全名首字母转小写
hive> create temporary function to_json as 'brickhouse.udf.json.ToJsonUDF' using jar 'hdfs:///udf/brickhouse-0.7.1-SNAPSHOT.jar';
 
# 查看刚注册的函数(函数会在相应的schema下，比如default.to_json)
show functions;

# 测试效果
hive> select to_json(ARRAY(MAP('a',1), MAP('b',2)));
OK
[{"a":1},{"b":2}]
```

# 报错说明
```bash
org.apache.hadoop.hive.ql.exec.UDFArgumentException:Don't know how to handle object inspector org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector@105efe24
```
- 针对to_json函数，目前不能转化Decimal类型的字段，如果源表有该类型的字段，会报错
- 可使用cast (`decimal_field` as string) 来解决



Welcome to the Brickhouse
=========================

[![Build Status](https://travis-ci.org/klout/brickhouse.svg?branch=master)](https://travis-ci.org/klout/brickhouse)

   Brickhouse is a collection of UDF's for Hive to improve developer 
   productivity, and the scalability and robustness of Hive queries.
   

  Brickhouse covers a wide range of functionality, grouped in the 
     following packages.

 * _collect_ - An implementaion of "collect"  and various utilities
     for dealing with maps and arrays.
   
 * _json_ - Translate between Hive structures and JSON strings

 * _sketch_ - An implementation of KMV sketch sets, for reach 
     estimation of large datasets.

 * _bloom_ - UDF wrappers around the Hadoop BloomFilter implementation.

 * _sanity_ - Tools for implementing sanity checks and managing Hive
	  in a production environment.
   
 * _hbase_ - Experimental UDFs for an alternative way to integrate
	  Hive with HBase.
     
Requirements:
--------------
  Brickhouse require Hive 0.9.0 or later;
  Maven 2.0 and a Java JDK is required to build.

Getting Started
---------------
 1. Clone ( or fork ) the repo from  https://github.com/klout/brickhouse 
 2. Run "mvn package" from the command line.
 3. Add the jar "target/brickhouse-\<version number\>.jar" to your HIVE_AUX_JARS_FILE_PATH,
    or add it to the distributed cache from the Hive CLI 
    with the "add jar" command
 4. Source the UDF declarations defined in src/main/resource/brickhouse.hql

See the wiki on Github at https://github.com/klout/brickhouse/wiki for more 
  information.

Also, see discussions on the Brickhouse Confessions blog on Wordpress 
 
 http://brickhouseconfessions.wordpress.com/
 

[![DOI](https://zenodo.org/badge/4948/klout/brickhouse.png)](http://dx.doi.org/10.5281/zenodo.10751)

