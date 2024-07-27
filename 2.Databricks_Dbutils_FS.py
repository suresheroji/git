# Databricks notebook source
# MAGIC %md
# MAGIC # select markdown or %md if you want to comment full cell
# MAGIC
# MAGIC
# MAGIC dbutils is a module in Databricks that provides utilities for working with Databricks file system (DBFS), 
# MAGIC for example, for reading and writing files, and for running shell commands. 
# MAGIC Some common methods of the dbutils module are:
# MAGIC
# MAGIC dbutils.fs: Provides methods for working with DBFS, such as cp (copy), rm (remove), ls (list), 
# MAGIC and mkdirs (make directories).
# MAGIC
# MAGIC Below will be covered once Pyspark is completed.
# MAGIC ******************************************************
# MAGIC dbutils.notebook.run
# MAGIC
# MAGIC dbutils.notebook: Provides methods for working with notebooks, such as run (run a notebook),
# MAGIC  exit (exit a notebook), and library (install or upload a library).
# MAGIC
# MAGIC dbutils.widgets: Provides methods for working with widgets, 
# MAGIC such as create (create a widget), get (get a widget), and delete (delete a widget).
# MAGIC
# MAGIC dbutils.secrets: Provides methods for managing secrets, 
# MAGIC such as get (get a secret value), and set (set a secret value).
# MAGIC
# MAGIC dbutils.library: Provides methods for managing libraries, 
# MAGIC such as install (install a library), uninstall (uninstall a library), and library (install or upload a library).
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Please note you have give correct DBFS path according to your DBFS .
# MAGIC example dbfs:/FileStore/siva/LoanDetails.csv   this exist in my DBFS, you have give your path. below are only for examples for syntax.
# MAGIC

# COMMAND ----------

# MAGIC %fs

# COMMAND ----------

# MAGIC
# MAGIC %fs cp dbfs:/FileStore/siva/LoanDetails.csv dbfs:/FileStore/LoanDetails_siva.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC create table suresh

# COMMAND ----------

# MAGIC %fs ls 
# MAGIC # creating directory
# MAGIC # creating file
# MAGIC # copying file
# MAGIC # removing file or directory 
# MAGIC

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %sql
# MAGIC create database sankar;

# COMMAND ----------

# MAGIC %fs rm /tmp/newdir

# COMMAND ----------

dbutils.fs.rm("/tmp/newdir",True)

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/siva/LoanDetails.csv

# COMMAND ----------

# hadoop Map Reduce - Apache Spark 
# hadoop File system (HDFS) - DBFS 
hadoop fs ls ,cp,rm,head,mkdir
# python can read files or write files in normal file system. it cannot access or read or write in DBFS or HDFS file system
# pyspark (spark) - can read or write from any file system. 

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

help(dbutils.fs)

# COMMAND ----------

#dbutils.fs.mkdirs("/FileStore/fs")
#dbutils.fs.put("/FileStore/fs/sampletext.txt","this is sample txt file which  iam using dbfs utilities to create a file")
#dbutils.fs.head("dbfs:/FileStore/fs/sampletext.txt")
#dbutils.fs.cp("dbfs:/FileStore/fs/sampletext.txt","dbfs:/FileStore/fs/copyfile.txt")
#dbutils.fs.mv("dbfs:/FileStore/fs/copyfile.txt","dbfs:/FileStore/fs/mvfile.txt")
dbutils.fs.rm("dbfs:/FileStore/fs",True)

# COMMAND ----------

# MAGIC %fs ls /FileStore/fs/

# COMMAND ----------

# cp - copy file 
# rm - removing file or directory 
# head- reading a file 
# put - placing file  or creating a file
# mkdir - creating directory
# mv - moving a file from one place to another place

# COMMAND ----------

dbutils.fs.put("/FileStore/tables/dbutils/dbutils.txt","this is sample file which i am using dbutils to create new file",True)

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/dbutils/dbutils.txt")

# COMMAND ----------

dbutils.fs.mv("/FileStore/tables/dbutils/dbutils.txt","/tmp/")

# COMMAND ----------

data ="""this is sample log information
i am storing into log file
this is another line
"""

# COMMAND ----------

# usernames and passwords 
dbutils.secrets.get(scope_name,kv_name)
# azure key vault - it will store all credentials.
# azure keyvault - can be used for storing connection strings, usernames,passwords,portnumbers.

# COMMAND ----------

dbutils.fs.put("/tmp/training/textfile.log",data,True)

# COMMAND ----------

dbutils.fs.rm("/tmp/training",True)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md #### DBUTILS  Commands

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

dbutils.fs.ls("")

# COMMAND ----------

# MAGIC %md
# MAGIC * mkdirs() - For Creating Directory
# MAGIC * cp() - For copying files from source location to target location and both locations will have files.
# MAGIC * ls - listing files 
# MAGIC * mv() - moving from source to target location. after moving only target location will have the file.
# MAGIC * put() - we can place any file in DBFS Location using put()
# MAGIC * rm() - removing files or folders - For multiple folders we need to use recursive True
# MAGIC * head() - reading header data from files..
# MAGIC

# COMMAND ----------

dbutils.fs.mv("dbfs:/bigdata/sample.text","/tmp/sample.text")

# COMMAND ----------

# MAGIC %fs ls dbfs:/bigdata/sample.text

# COMMAND ----------

# -r 
dbutils.fs.put("/bigdata/sample.text","this is sample file which i am creating using dbutils.fs.put")

# COMMAND ----------

dbutils.fs.ls("/bigdata")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating Directory using `mkdirs()` in `dbutils.fs.mkdirs('path')`

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/Training_Databricks/")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `ls`  listing files usding `dbutils.fs.ls('path')`

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

dbutils.fs.put("/FileStore/Training_Databricks/samplefile.txt","this is a sample text file creating using dbutils.fs.put")

# COMMAND ----------

dbutils.fs.head('/FileStore/Training_Databricks/samplefile.txt')

# COMMAND ----------

# MAGIC %fs  ls /tmp

# COMMAND ----------

display(dbutils.fs.ls("/tmp"))

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs ls /tmp/
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####  mkdirs() -for creating directory

# COMMAND ----------

dbutils.fs.put("/tmp/ravi/sample.txt","""<?xml version="1.0"?>
<xs:schema version="1.2" attributeFormDefault="unqualified" elementFormDefault="qualified"
    xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns="serviceaccountrelation" targetNamespace="serviceaccountrelation">
  <xs:element name="serviceAccounts">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="accounts" minOccurs="0" maxOccurs="unbounded">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="relationShpType" type="xs:string" minOccurs="0" maxOccurs="1"/>
              <xs:element name="accountNbr" type="xs:string" minOccurs="0" maxOccurs="1"/>
			  <xs:element name="productNbr" type="xs:string" minOccurs="0" maxOccurs="1"/>
              <xs:element name="accountSystemCd" type="xs:string" minOccurs="0" maxOccurs="1"/>
			  <xs:element name="bankNbr" type="xs:string" minOccurs="0" maxOccurs="1"/>
              <xs:element name="service" minOccurs="0" maxOccurs="unbounded">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="serviceId" type="xs:string"/>
                    <xs:element name="serviceAcctRlshpCd" type="xs:string" minOccurs="0" maxOccurs="1"/>
                    <xs:element name="accountSvcRlshpCd" type="xs:string" minOccurs="0" maxOccurs="1"/>
                    <xs:element name="relationExpDt" type="xs:string" minOccurs="0" maxOccurs="1"/>
                    <xs:element name="relationEffDt" type="xs:string" minOccurs="0" maxOccurs="1"/>
                    <xs:element name="serviceSystemCd" type="xs:string" minOccurs="0" maxOccurs="1"/>
                    <xs:element name="operationType" type="xs:string" minOccurs="0" maxOccurs="1"/>
                  </xs:sequence>
                </xs:complexType>
                <xs:key name="PKServiceId">
                  <xs:selector xpath="serviceAccounts"/>
                  <xs:field xpath="accountNbr"/>
                </xs:key>
              </xs:element>
              <xs:element name="accountAttributes" minOccurs="0" maxOccurs="unbounded">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="accountAttrType" type="xs:string" minOccurs="0" maxOccurs="1"/>
                    <xs:element name="accountAttrVal" type="xs:string" minOccurs="0" maxOccurs="1"/>
                  </xs:sequence>
                </xs:complexType>
                <xs:key name="PKAttribute">
                  <xs:selector xpath="serviceAccounts"/>
                  <xs:field xpath="accountNbr"/>
                </xs:key>
              </xs:element>
              <xs:element name="auditInfoAttributes" minOccurs="0" maxOccurs="unbounded">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="auditInfoType" type="xs:string"/>
                    <xs:element name="auditInfoVal" type="xs:string"/>
                  </xs:sequence>
                </xs:complexType>
                <xs:key name="PKAudits">
                  <xs:selector xpath="serviceAccounts"/>
                  <xs:field xpath="accountNbr"/>
                </xs:key>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>""",True)

# COMMAND ----------

dbutils.fs.cp("dbfs:/tmp/ravi/sample.txt","dbfs:/tmp/ravi_copy/sample_copy.txt",True)

# COMMAND ----------

dbutils.fs.cp("dbfs:/tmp/ravi_copy/","/temp/removdir/mydir",True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/temp/removdir/",True)

# COMMAND ----------

display(dbutils.fs.ls("/temp/removdir"))

# COMMAND ----------

dbutils.fs.mkdirs("/temp/removdir/mydir")

# COMMAND ----------

dbutils.fs.ls("dbfs:/tmp/databricks_dir/")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")


# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/tables/emp.csv","dbfs:/tmp/databricks_dir/")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating file using `put` in `dbutils.fs.put('filepath','use True for overwrite')`

# COMMAND ----------

dbutils.fs.put("dbfs:/tmp/mydir/sample.txt","this is sample text file creation",True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading file using `head` method.

# COMMAND ----------

dbutils.fs.head("dbfs:/tmp/mydir/sample.txt")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/tmp/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Removing Directory or File using `rm` method in `dbutils.fs.rm('file or directory',True)`
# MAGIC * Recursive parameter `True` for deleting recursively subfolders and non emtpy folders 

# COMMAND ----------

dbutils.fs.rm('dbfs:/tmp/mydir/',True)

# COMMAND ----------

display(dbutils.fs.ls('/tmp/test_data/'))

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

#display(dbutils.fs.ls("/foobar/"))

# COMMAND ----------

dbutils.fs.put("/foobar/baz.txt", "Hello, World!",True)


# COMMAND ----------

dbutils.fs.head("/foobar/baz.txt")

# COMMAND ----------

dbutils.fs.mkdirs("/foobar/")
dbutils.fs.put("/foobar/baz.txt", "Hello, World!",True)
dbutils.fs.head("/foobar/baz.txt")
display(dbutils.fs.ls("dbfs:/foobar"))
dbutils.fs.rm("/foobar/baz.txt")


# COMMAND ----------

#calling one notebook into another notebook

# COMMAND ----------

pip install 

# COMMAND ----------

dbutils.notebook.run('./Tutorial_1_Introduction',60)

# COMMAND ----------

# MAGIC %md #### Run external notebooks
# MAGIC * Notebooks to be run need to be copied to the remote filesystem, e.g. /dbfs/home/<username>/... with /dbfs/ 
# MAGIC   * being the posix pount of the dbfs   filesystem. Notebooks can be copied to remote dbfs with the databricks utility, e.g.
# MAGIC
# MAGIC * databricks --profile demo fs cp initialize.ipynb /dbfs/home/bernhard/
# MAGIC
# MAGIC * Since this notebook runs on the remote machine, this cannot be done in this notebook

# COMMAND ----------

# MAGIC %md #### Calling one notebook into another notebook.
# MAGIC *  `%run  notebook_name` calling one notebook into another notebook using `%run` command

# COMMAND ----------

# MAGIC %md ##### DBFS root
# MAGIC * The default storage location in DBFS is known as the DBFS root. Several types of data are stored in the following DBFS root locations:
# MAGIC
# MAGIC * /FileStore: Imported data files, generated plots, and uploaded libraries. See FileStore.
# MAGIC * /databricks-datasets: Sample public datasets.
# MAGIC * /databricks-results: Files generated by downloading the full results of a query.
# MAGIC * /databricks/init: Global and cluster-named (deprecated) init scripts.
# MAGIC * /user/hive/warehouse: Data and metadata for non-external Hive tables.

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets/learning-spark/data-001/')

# COMMAND ----------

# MAGIC %md * Alternatively, you can use %fs:

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md * Notebook workflow utilities

# COMMAND ----------

# MAGIC %md * install library's  using ```dbutils```

# COMMAND ----------

dbutils.library.installPyPI("torch")
dbutils.library.installPyPI("azureml-sdk", extras="databricks")
dbutils.library.restartPython()  
# Removes Python state, but some libraries might not work without calling this function

# COMMAND ----------

# MAGIC %md * get list of library's in databricks ```list()```

# COMMAND ----------

dbutils.fs.head("/ravi/files/emp.csv")

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')
