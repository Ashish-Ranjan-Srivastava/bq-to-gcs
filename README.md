# bq-to-gcs

Aim of the pipeline -  Reading data from bigquery table and loading the data into gcs bucket in Json Format.

Approach -  Reading the data from BQ using 
            pipeline
                .apply(
                        "Read from BigQuery query",
                        BigQueryIO.readTableRows()
                                .fromQuery(String.format("SELECT Name FROM `%s.%s.%s`", project, dataset, table))
                                .usingStandardSql()
return data type will be TableRow

Now have to convert TableRow into Json and the load it to GCS bucket.

For now Everything is hardcoded, we can use option method to make it dynamic.

Executable Command - mvn compile exec:java -Dexec.mainClass=com.google.cloud.teleport.templates.BigQueryReadFromQuery -Dexec.cleanupDaemonThreads=false -Dexec.args="--project=ProjectName "

Note - we need to assign tempgcs Location for the temporaray Location used by beam Runner
