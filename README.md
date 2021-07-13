# bq-to-gcs

Aim of the pipeline -  Reading data from bigquery table and loading the data into gcs bucket in Json Format.

Approach -  Reading the data from BQ using 
            pipeline
                .apply(
                        "Read from BigQuery query",
                        BigQueryIO.readTableRows()
                                .from(String.format("%s:%s.%s", project, dataset, table))
                                .usingStandardSql()
return data type will be TableRow

Now have to convert TableRow into Json and the load it to GCS bucket.


Executable Command - mvn compile exec:java -Dexec.mainClass=test -Dexec.cleanupDaemonThreads=false -Dexec.args="--project=ProjectID --dataset=BQ DataSet --projectName=Your Project Name --table=Table Name --storageLocation=Destination Bucket "


Note - we need to assign tempgcs Location for the temporaray Location used by beam Runner



**DAG formed on DataFlow console** 
![image](https://user-images.githubusercontent.com/47782446/125329454-22cc1680-e363-11eb-930c-a4fea7a5e602.png)

