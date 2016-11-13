This Readme has been automatically created by [zepppelin2md.py](https://github.com/bernhard-42/zeppelin2md).

Alternatively open the Zeppelin Notebook json file (spark-unzip.json) with [Zeppelin Hub Viewer](https://www.zeppelinhub.com/viewer):

- click on spark-unzip.json
- click on RAW
- copy URL to into Zeppelin Hub Viewer

---

#### As always, check the Spark version

_Input:_

```scala
%spark
sc.version
```


_Result:_

```
res29: String = 1.6.2

```

---

# 1) ZIP compressed data

ZIP compression format is not splittable and there is no default input format defined in Hadoop. To read ZIP files, Hadoop needs to be informed that it this file type is not splittable and needs an appropriate record reader, see [Hadoop: Processing ZIP files in Map/Reduce](http://cutler.io/2012/07/hadoop-processing-zip-files-in-mapreduce/).

In order to work with ZIP files in Zeppelin, follow the installation instructions in the appendix of this notebook


---

#### Six zip files containing XML records are placed below /tmp/zip

_Input:_

```bash
%sh

echo "Folder:"
hdfs dfs -ls /tmp/zip
echo " "
echo "XML records:"
hdfs dfs -get /tmp/zip/logfile_1.zip /tmp/l.zip
unzip -p /tmp/l.zip | head -n 3
```


_Result:_

```
Folder:
Found 6 items
-rw-r--r--   3 bernhard hdfs       3764 2016-11-11 17:55 /tmp/zip/logfile_1.zip
-rw-r--r--   3 bernhard hdfs       3833 2016-11-11 17:55 /tmp/zip/logfile_2.zip
-rw-r--r--   3 bernhard hdfs       3796 2016-11-11 17:55 /tmp/zip/logfile_3.zip
-rw-r--r--   3 bernhard hdfs       3757 2016-11-11 17:55 /tmp/zip/logfile_4.zip
-rw-r--r--   3 bernhard hdfs       3841 2016-11-11 17:55 /tmp/zip/logfile_5.zip
-rw-r--r--   3 bernhard hdfs       3712 2016-11-11 17:55 /tmp/zip/logfile_6.zip
 
XML records:
<?xml version="1.0" encoding="UTF-8" ?><root><name type="dict"><first_name type="str">Heinz Dieter</first_name><name type="str">Antonino Döhn</name></name><address type="dict"><city type="str">Iserlohn</city><street type="str">Erich-Roskoth-Straße</street><country type="str">Deutschland</country></address></root>
<?xml version="1.0" encoding="UTF-8" ?><root><name type="dict"><first_name type="str">Marta</first_name><name type="str">Hedy Kobelt</name></name><address type="dict"><city type="str">Jena</city><street type="str">Slavko-Karge-Gasse</street><country type="str">Deutschland</country></address></root>
<?xml version="1.0" encoding="UTF-8" ?><root><name type="dict"><first_name type="str">Amalia</first_name><name type="str">Sibylle Jacobi Jäckel</name></name><address type="dict"><city type="str">Böblingen</city><street type="str">Teresa-Meister-Allee</street><country type="str">Deutschland</country></address></root>

```

---

#### Empty the results folder

_Input:_

```bash
%sh
hdfs dfs -rm -r /tmp/results
hdfs dfs -mkdir /tmp/results
```


---

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

#### Parse an XML record and return it as a scala case class instance

_Input:_

```scala
%spark
case class Person(first_name:String, name:String, street:String, city:String, country:String)

def parseXML(xmlStr: String) = {
    val xml = XML.loadString(xmlStr)
    val first_name = (xml \ "name" \ "first_name").text
    val name = (xml \ "name" \ "name").text
    val street = (xml \ "address" \ "street").text
    val city = (xml \ "address" \ "city").text
    val country = (xml \ "address" \ "country").text

    Person(first_name, name, street, city, country)
}
```


_Result:_

```
defined class Person
parseXML: (xmlStr: String)Person

```

---

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
zipFileRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[40] at map at <console>:46

```

---

#### ... and parse them into a DataFrame using the case class

_Input:_

```scala
%spark
val df = zipFileRDD.flatMap { _.split("\n") }
                   .map(parseXML)
                   .toDF
```


_Result:_

```
df: org.apache.spark.sql.DataFrame = [first_name: string, name: string, street: string, city: string, country: string]

```

---

#### Show the result

_Input:_

```scala
%spark
df.show(10)
```


_Result:_

```
+------------+--------------------+--------------------+----------+-----------+
|  first_name|                name|              street|      city|    country|
+------------+--------------------+--------------------+----------+-----------+
|Heinz Dieter|       Antonino Döhn|Erich-Roskoth-Straße|  Iserlohn|Deutschland|
|       Marta|         Hedy Kobelt|  Slavko-Karge-Gasse|      Jena|Deutschland|
|      Amalia|Sibylle Jacobi Jä...|Teresa-Meister-Allee| Böblingen|Deutschland|
|      Gerwin|   Pauline Kade B.A.|  Niels-Winkler-Ring|Biedenkopf|Deutschland|
|        Ines|Norma Binner-Margraf|           Meyerring|   Wolgast|Deutschland|
|     Cynthia|       Emmerich Metz|  Sandy-Atzler-Allee|  Grafenau|Deutschland|
|   Felicitas|Frau Cosima Döhn ...|Brunhilde-Naser-Ring| Gadebusch|Deutschland|
|    Hannchen|Ulrike Bender-Sei...|Frida-Möchlichen-...|    Döbeln|Deutschland|
|    Frithjof| Niklas Linke B.Eng.|         Lindauplatz| Sternberg|Deutschland|
|     Chantal|Heinfried Roskoth...|Dorothee-Rosemann...|  Burgdorf|Deutschland|
+------------+--------------------+--------------------+----------+-----------+
only showing top 10 rows


```

---

#### Save the records as ORC and as Parquet files

_Input:_

```scala
%spark
df.write.format("orc").save("/tmp/results/people.orc")
df.write.format("parquet").save("/tmp/results/people.parquet")
```


---

#### According to the number of partitions in Spark several files will be created by Spark

_Input:_

```bash
%sh
hdfs dfs -ls /tmp/results/people.orc
hdfs dfs -ls /tmp/results/people.parquet
```


_Result:_

```
Found 7 items
-rw-r--r--   3 zeppelin hdfs          0 2016-11-13 18:08 /tmp/results/people.orc/_SUCCESS
-rw-r--r--   3 zeppelin hdfs       4036 2016-11-13 18:08 /tmp/results/people.orc/part-r-00000-465ac1ad-3a48-4b15-9601-b066af702212.orc
-rw-r--r--   3 zeppelin hdfs       4090 2016-11-13 18:08 /tmp/results/people.orc/part-r-00001-465ac1ad-3a48-4b15-9601-b066af702212.orc
-rw-r--r--   3 zeppelin hdfs       4051 2016-11-13 18:08 /tmp/results/people.orc/part-r-00002-465ac1ad-3a48-4b15-9601-b066af702212.orc
-rw-r--r--   3 zeppelin hdfs       4014 2016-11-13 18:08 /tmp/results/people.orc/part-r-00003-465ac1ad-3a48-4b15-9601-b066af702212.orc
-rw-r--r--   3 zeppelin hdfs       4125 2016-11-13 18:08 /tmp/results/people.orc/part-r-00004-465ac1ad-3a48-4b15-9601-b066af702212.orc
-rw-r--r--   3 zeppelin hdfs       4027 2016-11-13 18:08 /tmp/results/people.orc/part-r-00005-465ac1ad-3a48-4b15-9601-b066af702212.orc
Found 9 items
-rw-r--r--   3 zeppelin hdfs          0 2016-11-13 18:08 /tmp/results/people.parquet/_SUCCESS
-rw-r--r--   3 zeppelin hdfs        538 2016-11-13 18:08 /tmp/results/people.parquet/_common_metadata
-rw-r--r--   3 zeppelin hdfs       4448 2016-11-13 18:08 /tmp/results/people.parquet/_metadata
-rw-r--r--   3 zeppelin hdfs       4913 2016-11-13 18:08 /tmp/results/people.parquet/part-r-00000-1b9890ba-1b08-4905-8826-911b5b6cc8b1.gz.parquet
-rw-r--r--   3 zeppelin hdfs       4954 2016-11-13 18:08 /tmp/results/people.parquet/part-r-00001-1b9890ba-1b08-4905-8826-911b5b6cc8b1.gz.parquet
-rw-r--r--   3 zeppelin hdfs       4933 2016-11-13 18:08 /tmp/results/people.parquet/part-r-00002-1b9890ba-1b08-4905-8826-911b5b6cc8b1.gz.parquet
-rw-r--r--   3 zeppelin hdfs       4897 2016-11-13 18:08 /tmp/results/people.parquet/part-r-00003-1b9890ba-1b08-4905-8826-911b5b6cc8b1.gz.parquet
-rw-r--r--   3 zeppelin hdfs       4957 2016-11-13 18:08 /tmp/results/people.parquet/part-r-00004-1b9890ba-1b08-4905-8826-911b5b6cc8b1.gz.parquet
-rw-r--r--   3 zeppelin hdfs       4844 2016-11-13 18:08 /tmp/results/people.parquet/part-r-00005-1b9890ba-1b08-4905-8826-911b5b6cc8b1.gz.parquet

```

---

#### Now repartition the DataFrame and store it again as ORC and Parquet

_Input:_

```scala
%spark
 df.repartition(1).write.format("orc").save("/tmp/results/people1.orc")
 df.repartition(1).write.format("parquet").save("/tmp/results/people1.parquet")
```


---

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
-rw-r--r--   3 zeppelin hdfs          0 2016-11-13 18:08 /tmp/results/people1.orc/_SUCCESS
-rw-r--r--   3 zeppelin hdfs      16601 2016-11-13 18:08 /tmp/results/people1.orc/part-r-00000-099f17e8-7e93-4e3a-9cba-95d8c1e013b4.orc
Found 4 items
-rw-r--r--   3 zeppelin hdfs          0 2016-11-13 18:08 /tmp/results/people1.parquet/_SUCCESS
-rw-r--r--   3 zeppelin hdfs        538 2016-11-13 18:08 /tmp/results/people1.parquet/_common_metadata
-rw-r--r--   3 zeppelin hdfs       1197 2016-11-13 18:08 /tmp/results/people1.parquet/_metadata
-rw-r--r--   3 zeppelin hdfs      18288 2016-11-13 18:08 /tmp/results/people1.parquet/part-r-00000-29a86cbd-79cb-4933-86d9-78e0fe06148c.gz.parquet

```

---


_Input:_

```scala
%spark
%md
# 2) GZIP compressed data

For gzip compressed data a default File input Format exists in Hadoop
```


---


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
Found 6 items
-rw-r--r--   3 bernhard hdfs       3564 2016-11-11 18:21 /tmp/gzip/logfile_1.gz
-rw-r--r--   3 bernhard hdfs       3632 2016-11-11 18:21 /tmp/gzip/logfile_2.gz
-rw-r--r--   3 bernhard hdfs       3609 2016-11-11 18:21 /tmp/gzip/logfile_3.gz
-rw-r--r--   3 bernhard hdfs       3561 2016-11-11 18:21 /tmp/gzip/logfile_4.gz
-rw-r--r--   3 bernhard hdfs       3638 2016-11-11 18:21 /tmp/gzip/logfile_5.gz
-rw-r--r--   3 bernhard hdfs       3514 2016-11-11 18:21 /tmp/gzip/logfile_6.gz
 
XML records:
<?xml version="1.0" encoding="UTF-8" ?><root><name type="dict"><first_name type="str">Heinz Dieter</first_name><name type="str">Antonino Döhn</name></name><address type="dict"><city type="str">Iserlohn</city><street type="str">Erich-Roskoth-Straße</street><country type="str">Deutschland</country></address></root>
<?xml version="1.0" encoding="UTF-8" ?><root><name type="dict"><first_name type="str">Marta</first_name><name type="str">Hedy Kobelt</name></name><address type="dict"><city type="str">Jena</city><street type="str">Slavko-Karge-Gasse</street><country type="str">Deutschland</country></address></root>
<?xml version="1.0" encoding="UTF-8" ?><root><name type="dict"><first_name type="str">Amalia</first_name><name type="str">Sibylle Jacobi Jäckel</name></name><address type="dict"><city type="str">Böblingen</city><street type="str">Teresa-Meister-Allee</street><country type="str">Deutschland</country></address></root>

```

---

#### Read all gzip files into an RDD ...

_Input:_

```scala
%spark
val zipFileRDD2 = sc.textFile("/tmp/gzip")
```


_Result:_

```
zipFileRDD2: org.apache.spark.rdd.RDD[String] = /tmp/gzip MapPartitionsRDD[56] at textFile at <console>:42

```

---

#### ... and (as above for zip files) parse them into a DataFrame using the case class

_Input:_

```scala
%spark
val df2 = zipFileRDD2.flatMap { _.split("\n") }
                     .map(parseXML)
                     .toDF
                   
```


_Result:_

```
df2: org.apache.spark.sql.DataFrame = [first_name: string, name: string, street: string, city: string, country: string]

```

---


_Input:_

```scala
%spark
df2.show(10)
```


_Result:_

```
+------------+--------------------+--------------------+----------+-----------+
|  first_name|                name|              street|      city|    country|
+------------+--------------------+--------------------+----------+-----------+
|Heinz Dieter|       Antonino Döhn|Erich-Roskoth-Straße|  Iserlohn|Deutschland|
|       Marta|         Hedy Kobelt|  Slavko-Karge-Gasse|      Jena|Deutschland|
|      Amalia|Sibylle Jacobi Jä...|Teresa-Meister-Allee| Böblingen|Deutschland|
|      Gerwin|   Pauline Kade B.A.|  Niels-Winkler-Ring|Biedenkopf|Deutschland|
|        Ines|Norma Binner-Margraf|           Meyerring|   Wolgast|Deutschland|
|     Cynthia|       Emmerich Metz|  Sandy-Atzler-Allee|  Grafenau|Deutschland|
|   Felicitas|Frau Cosima Döhn ...|Brunhilde-Naser-Ring| Gadebusch|Deutschland|
|    Hannchen|Ulrike Bender-Sei...|Frida-Möchlichen-...|    Döbeln|Deutschland|
|    Frithjof| Niklas Linke B.Eng.|         Lindauplatz| Sternberg|Deutschland|
|     Chantal|Heinfried Roskoth...|Dorothee-Rosemann...|  Burgdorf|Deutschland|
+------------+--------------------+--------------------+----------+-----------+
only showing top 10 rows


```

---


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

