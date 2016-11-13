>**Note:**
>This Readme has been automatically created by [zepppelin2md.py](https://github.com/bernhard-42/zeppelin2md).

>Alternatively, to open the Zeppelin Notebook with [Zeppelin Viewer](https://www.zeppelinhub.com/viewer) use the URL 
>    `https://raw.githubusercontent.com/bernhard-42/spark-unzip/master/spark-unzip.json`

# spark-unzip.json

---

scala
#### As always, check the Spark version

_Input:_

```scala
%spark
sc.version
```


_Result:_

```
res47: String = 1.6.2

```

---

markdown
# 1) ZIP compressed data

ZIP compression format is not splittable and there is no default input format defined in Hadoop. To read ZIP files, Hadoop needs to be informed that it this file type is not splittable and needs an appropriate record reader, see [Hadoop: Processing ZIP files in Map/Reduce](http://cutler.io/2012/07/hadoop-processing-zip-files-in-mapreduce/).

In order to work with ZIP files in Zeppelin, follow the installation instructions in the `Appendix` of this notebook

Test data can be created with `data/create-data.sh`


---

sh
#### Six zip files containing XML records are placed below /tmp/zip

_Input:_

```bash
%sh

echo "Folder:"
hdfs dfs -ls /tmp/zip
echo " "

echo "ZIP Files"
rm -f /tmp/l.zip
hdfs dfs -get /tmp/zip/logfiles1.zip /tmp/l.zip
unzip -l /tmp/l.zip
echo " "

echo "XML records:"
unzip -p /tmp/l.zip | head -n 3
```


_Result:_

```
Folder:
Found 3 items
-rw-r--r--   3 bernhard hdfs     712729 2016-11-13 19:43 /tmp/zip/logfiles1.zip
-rw-r--r--   3 bernhard hdfs     713130 2016-11-13 19:43 /tmp/zip/logfiles2.zip
-rw-r--r--   3 bernhard hdfs     711966 2016-11-13 19:43 /tmp/zip/logfiles3.zip
 
ZIP Files
Archive:  /tmp/l.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
  2313784  2016-11-13 19:37   logfile_0
  2314604  2016-11-13 19:37   logfile_1
  2313592  2016-11-13 19:37   logfile_2
---------                     -------
  6941980                     3 files
 
XML records:
<?xml version="1.0" encoding="UTF-8" ?><root><name><first_name>Constance</first_name><last_name>Schaaf</last_name></name><address><city>Neubrandenburg</city><street>Carmine-Löchel-Weg</street><country>Deutschland</country></address></root>
<?xml version="1.0" encoding="UTF-8" ?><root><name><first_name>Rosalie</first_name><last_name>Lorch</last_name></name><address><city>Rastatt</city><street>Schachtring</street><country>Deutschland</country></address></root>
<?xml version="1.0" encoding="UTF-8" ?><root><name><first_name>Max</first_name><last_name>Radisch</last_name></name><address><city>Nordhausen</city><street>Ditschlerinweg</street><country>Deutschland</country></address></root>

```

---

sh
#### Empty the results folder

_Input:_

```bash
%sh
hdfs dfs -rm -r /tmp/results
hdfs dfs -mkdir /tmp/results
```


---

scala
#### Import all necessary symbols

_Input:_

```scala
%spark
import com.cotdp.hadoop.ZipFileInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}
import scala.xml.XML
```


_Result:_

```
import com.cotdp.hadoop.ZipFileInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}
import scala.xml.XML

```

---

scala
#### Parse an XML record and return it as a scala case class instance

_Input:_

```scala
%spark
case class Person(first_name:String, last_name:String, street:String, city:String, country:String)

def parseXML(xmlStr: String) = {
    val xml = XML.loadString(xmlStr)
    val first_name = (xml \ "name" \ "first_name").text
    val last_name = (xml \ "name" \ "last_name").text
    val street = (xml \ "address" \ "street").text
    val city = (xml \ "address" \ "city").text
    val country = (xml \ "address" \ "country").text

    Person(first_name, last_name, street, city, country)
}
```


_Result:_

```
defined class Person
parseXML: (xmlStr: String)Person

```

---

scala
#### Read all zip files into an RDD ... 

_Input:_

```scala
%spark
val zipFileRDD = sc.newAPIHadoopFile("/tmp/zip", classOf[ZipFileInputFormat],
                                                 classOf[Text], 
                                                 classOf[BytesWritable], 
                                                 sc.hadoopConfiguration)
                   .map { case(a,b) => new String( b.getBytes(), "UTF-8" ) }
```


_Result:_

```
zipFileRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[62] at map at <console>:53

```

---

scala
#### ... and parse them into a DataFrame using the case class

_Input:_

```scala
%spark
val df = zipFileRDD.flatMap { _.split("\n") }
                   .map(parseXML)
                   .toDF
df.count
```


_Result:_

```
df: org.apache.spark.sql.DataFrame = [first_name: string, last_name: string, street: string, city: string, country: string]
res53: Long = 90000

```

---

scala
#### Show the result

_Input:_

```scala
%spark
df.show(10)
```


_Result:_

```
+----------+-----------+--------------------+--------------+-----------+
|first_name|  last_name|              street|          city|    country|
+----------+-----------+--------------------+--------------+-----------+
| Constance|     Schaaf|  Carmine-Löchel-Weg|Neubrandenburg|Deutschland|
|   Rosalie|      Lorch|         Schachtring|       Rastatt|Deutschland|
|       Max|    Radisch|      Ditschlerinweg|    Nordhausen|Deutschland|
|     Ahmed|    Heintze|      Cichoriusgasse|        Amberg|Deutschland|
|   Antonia|      Gunpf| Kreszentia-Bähr-Weg|     Warendorf|Deutschland|
|     Janko|  Christoph|Jonas-Röhrdanz-Allee|    Gelnhausen|Deutschland|
|    Ottmar|     Seidel|Ernestine-Hornich...|     Wunsiedel|Deutschland|
|   Mattias|Wagenknecht|         Kästergasse|      Arnstadt|Deutschland|
|    Marika|    Scholtz|       Benthinstraße|       Ilmenau|Deutschland|
|     Jobst|      Rogge|           Zimmerweg|   Sigmaringen|Deutschland|
+----------+-----------+--------------------+--------------+-----------+
only showing top 10 rows


```

---

scala
#### Save the records as ORC and as Parquet files

_Input:_

```scala
%spark
df.write.format("orc").save("/tmp/results/people.orc")
df.write.format("parquet").save("/tmp/results/people.parquet")
```


---

sh
#### According to the number of partitions in Spark several files will be created by Spark

_Input:_

```bash
%sh
hdfs dfs -ls /tmp/results/people.orc
hdfs dfs -ls /tmp/results/people.parquet
```


_Result:_

```
Found 4 items
-rw-r--r--   3 zeppelin hdfs          0 2016-11-13 19:50 /tmp/results/people.orc/_SUCCESS
-rw-r--r--   3 zeppelin hdfs     308634 2016-11-13 19:49 /tmp/results/people.orc/part-r-00000-b1f95464-e242-485c-8e6d-e9a927efbe4a.orc
-rw-r--r--   3 zeppelin hdfs     308621 2016-11-13 19:49 /tmp/results/people.orc/part-r-00001-b1f95464-e242-485c-8e6d-e9a927efbe4a.orc
-rw-r--r--   3 zeppelin hdfs     308911 2016-11-13 19:50 /tmp/results/people.orc/part-r-00002-b1f95464-e242-485c-8e6d-e9a927efbe4a.orc
Found 6 items
-rw-r--r--   3 zeppelin hdfs          0 2016-11-13 19:51 /tmp/results/people.parquet/_SUCCESS
-rw-r--r--   3 zeppelin hdfs        548 2016-11-13 19:51 /tmp/results/people.parquet/_common_metadata
-rw-r--r--   3 zeppelin hdfs       2559 2016-11-13 19:51 /tmp/results/people.parquet/_metadata
-rw-r--r--   3 zeppelin hdfs     309015 2016-11-13 19:51 /tmp/results/people.parquet/part-r-00000-56a9dae0-b285-40e1-bf2e-2f6beb73b16d.gz.parquet
-rw-r--r--   3 zeppelin hdfs     309185 2016-11-13 19:51 /tmp/results/people.parquet/part-r-00001-56a9dae0-b285-40e1-bf2e-2f6beb73b16d.gz.parquet
-rw-r--r--   3 zeppelin hdfs     308872 2016-11-13 19:51 /tmp/results/people.parquet/part-r-00002-56a9dae0-b285-40e1-bf2e-2f6beb73b16d.gz.parquet

```

---

scala
#### Now repartition the DataFrame and store it again as ORC and Parquet

_Input:_

```scala
%spark
 df.repartition(1).write.format("orc").save("/tmp/results/people1.orc")
 df.repartition(1).write.format("parquet").save("/tmp/results/people1.parquet")
```


---

sh
#### There is one content file per saved ORC or Parquet folder in HDFS now

_Input:_

```bash
%sh

hdfs dfs -ls /tmp/results/people1.orc
hdfs dfs -ls /tmp/results/people1.parquet
```


_Result:_

```
Found 2 items
-rw-r--r--   3 zeppelin hdfs          0 2016-11-13 19:54 /tmp/results/people1.orc/_SUCCESS
-rw-r--r--   3 zeppelin hdfs     858603 2016-11-13 19:54 /tmp/results/people1.orc/part-r-00000-870982f1-bc81-47d3-8868-6e337edf300f.orc
Found 4 items
-rw-r--r--   3 zeppelin hdfs          0 2016-11-13 19:55 /tmp/results/people1.parquet/_SUCCESS
-rw-r--r--   3 zeppelin hdfs        548 2016-11-13 19:55 /tmp/results/people1.parquet/_common_metadata
-rw-r--r--   3 zeppelin hdfs       1222 2016-11-13 19:55 /tmp/results/people1.parquet/_metadata
-rw-r--r--   3 zeppelin hdfs     886734 2016-11-13 19:55 /tmp/results/people1.parquet/part-r-00000-6ef20812-6e2f-4356-9837-f15021e2f9a0.gz.parquet

```

---

markdown

# 2) GZIP compressed data

For gzip compressed data a default File input Format exists in Hadoop


---

sh

_Input:_

```bash
%sh

echo "Folder:"
hdfs dfs -ls /tmp/gzip
echo " "
echo "XML records:"
hdfs dfs -cat /tmp/gzip/logfile_1.gz | gzip -dc | head -n 3
```


_Result:_

```
Folder:
Found 9 items
-rw-r--r--   3 bernhard hdfs     230579 2016-11-13 19:56 /tmp/gzip/logfile_0.gz
-rw-r--r--   3 bernhard hdfs     231281 2016-11-13 19:56 /tmp/gzip/logfile_1.gz
-rw-r--r--   3 bernhard hdfs     230567 2016-11-13 19:56 /tmp/gzip/logfile_2.gz
-rw-r--r--   3 bernhard hdfs     231273 2016-11-13 19:56 /tmp/gzip/logfile_3.gz
-rw-r--r--   3 bernhard hdfs     230518 2016-11-13 19:56 /tmp/gzip/logfile_4.gz
-rw-r--r--   3 bernhard hdfs     231085 2016-11-13 19:56 /tmp/gzip/logfile_5.gz
-rw-r--r--   3 bernhard hdfs     230767 2016-11-13 19:56 /tmp/gzip/logfile_6.gz
-rw-r--r--   3 bernhard hdfs     230531 2016-11-13 19:56 /tmp/gzip/logfile_7.gz
-rw-r--r--   3 bernhard hdfs     230467 2016-11-13 19:56 /tmp/gzip/logfile_8.gz
 
XML records:
<?xml version="1.0" encoding="UTF-8" ?><root><name><first_name>Abram</first_name><last_name>Loos</last_name></name><address><city>Mellrichstadt</city><street>Mikhail-Scholl-Allee</street><country>Deutschland</country></address></root>
<?xml version="1.0" encoding="UTF-8" ?><root><name><first_name>Sylvester</first_name><last_name>Hübel</last_name></name><address><city>Eichstätt</city><street>Paulina-Ortmann-Allee</street><country>Deutschland</country></address></root>
<?xml version="1.0" encoding="UTF-8" ?><root><name><first_name>Sandy</first_name><last_name>Schulz</last_name></name><address><city>Freudenstadt</city><street>Felicia-Hermighausen-Gasse</street><country>Deutschland</country></address></root>

```

---

scala
#### Read all gzip files into an RDD ...

_Input:_

```scala
%spark
val zipFileRDD2 = sc.textFile("/tmp/gzip")
```


_Result:_

```
zipFileRDD2: org.apache.spark.rdd.RDD[String] = /tmp/gzip MapPartitionsRDD[84] at textFile at <console>:49

```

---

scala
#### ... and (as above for zip files) parse them into a DataFrame using the case class

_Input:_

```scala
%spark
val df2 = zipFileRDD2.flatMap { _.split("\n") }
                     .map(parseXML)
                     .toDF
df.count
```


_Result:_

```
df2: org.apache.spark.sql.DataFrame = [first_name: string, last_name: string, street: string, city: string, country: string]
res64: Long = 90000

```

---

scala

_Input:_

```scala
%spark
df2.show(10)
```


_Result:_

```
+----------+-----------+--------------------+--------------+-----------+
|first_name|  last_name|              street|          city|    country|
+----------+-----------+--------------------+--------------+-----------+
| Constance|     Schaaf|  Carmine-Löchel-Weg|Neubrandenburg|Deutschland|
|   Rosalie|      Lorch|         Schachtring|       Rastatt|Deutschland|
|       Max|    Radisch|      Ditschlerinweg|    Nordhausen|Deutschland|
|     Ahmed|    Heintze|      Cichoriusgasse|        Amberg|Deutschland|
|   Antonia|      Gunpf| Kreszentia-Bähr-Weg|     Warendorf|Deutschland|
|     Janko|  Christoph|Jonas-Röhrdanz-Allee|    Gelnhausen|Deutschland|
|    Ottmar|     Seidel|Ernestine-Hornich...|     Wunsiedel|Deutschland|
|   Mattias|Wagenknecht|         Kästergasse|      Arnstadt|Deutschland|
|    Marika|    Scholtz|       Benthinstraße|       Ilmenau|Deutschland|
|     Jobst|      Rogge|           Zimmerweg|   Sigmaringen|Deutschland|
+----------+-----------+--------------------+--------------+-----------+
only showing top 10 rows


```

---

markdown

# Appendix) Add ZipFileInputFormat to HDP 2.5

## Build ZipFileInputFormat

In order to get the ZIP Input Format running with HDP 2.5, the repository needs to be downloaded first (`git clone https://github.com/cotdp/com-cotdp-hadoop.git`)

Then `pom.xml` needs to updated to reflect the library versions of HDP 2.5:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.cotdp.hadoop</groupId>
    <artifactId>com-cotdp-hadoop</artifactId>
    <packaging>jar</packaging>
    <version>1.0-SNAPSHOT</version>
    <name>com-cotdp-hadoop</name>
    <url>http://cotdp.com/</url>
    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>
    <repositories>
        <!-- HDP Repositories -->
        <repository>
            <releases>
                <enabled>true</enabled>
                <updatePolicy>always</updatePolicy>
                <checksumPolicy>warn</checksumPolicy>
            </releases>
            <snapshots>
            <enabled>false</enabled>
            <updatePolicy>never</updatePolicy>
            <checksumPolicy>fail</checksumPolicy>
            </snapshots>
            <id>HDPReleases</id>
            <name>HDP Releases</name>
            <url>http://repo.hortonworks.com/content/repositories/releases/</url>
            <layout>default</layout>
        </repository>
        <repository>
            <releases>
                <enabled>true</enabled>
                <updatePolicy>always</updatePolicy>
                <checksumPolicy>warn</checksumPolicy>
            </releases>
            <snapshots>
                <enabled>true</enabled>
                <updatePolicy>never</updatePolicy>
                <checksumPolicy>fail</checksumPolicy>
            </snapshots>
            <id>HDPPublic</id>
            <name>HDP Public</name>
            <url>http://repo.hortonworks.com/content/repositories/public/</url>
            <layout>default</layout>
        </repository>
    </repositories>
    <dependencies>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.7.3.2.5.0.0-1245</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>2.7.3.2.5.0.0-1245</version>
        </dependency>
        <dependency>
            <groupId>org.codehaus.jackson</groupId>
            <artifactId>jackson-mapper-asl</artifactId>
        <version>1.9.8</version>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>3.8.1</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

Finally it can be compiled and packaged:

```shell
mvn package
```

## Add ZipFileInputFormat to Apache Zeppelin

Now copy `target/com-cotdp-hadoop-1.0-SNAPSHOT.jar` to the machine where Zeppeliln Server is installed, e.g. to `/tmp/com-cotdp-hadoop-1.0-SNAPSHOT.jar`

Open the Interpreter settings under `<logged-in-user> - Interpreter`, edit `/tmp/com-cotdp-hadoop-1.0-SNAPSHOT.jar` as an artifact below `Dependencies`

Restart the Spark Interpreter and `ZipFileInputformat` can be used

