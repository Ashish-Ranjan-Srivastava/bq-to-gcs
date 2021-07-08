
package com.google.cloud.teleport.templates;
//import org.apache.beam.examples.snippets.transforms.io.gcp.bigquery.BigQueryMyData.MyData;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;



public class BigQueryReadFromQuery {
    public interface bq_option extends PipelineOptions {
//         @Description("Path of the file to read from")
//         ValueProvider<String> Project();
//         void Project(ValueProvider<String> value);

    }

    public static void main(String[] args)
    {
        bq_option options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(bq_option.class);

        System.out.println("checking Library");
        String project = "gcp-training-poc";
        String dataset = "test";
         String table = "user";
//	options.setProjectId("gcp-training-poc");
        Pipeline pipeline = Pipeline.create(options);
        options.setTempLocation("gs://dev-v2/temp");
//	options.setprojectId("gcp-training-poc");
        PCollection<TableRow> rows = pipeline
                .apply(
                        "Read from BigQuery query",
                        BigQueryIO.readTableRows()
                                .fromQuery(String.format("SELECT Name FROM `%s.%s.%s`", project, dataset, table))
                                .usingStandardSql()
                );


        rows.apply(ParDo.of(new DoFn<TableRow, Void>() {
            @ProcessElement
            public void ProcessElement(ProcessContext c)
            {
                System.out.println("Trying to print the data");
                System.out.println(c.element() + "--->" + c.element().getClass().getName());
            }
        }));
        System.out.println(rows.getName());
        System.out.println(rows.getTypeDescriptor());
        PipelineResult result = pipeline.run();

  //  rows.apply(TextIO.write().to("gs://dev-v2//check,txt"));
    }


}
