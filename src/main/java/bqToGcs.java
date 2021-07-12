

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import java.util.logging.*;


/*
This script will load data from bigquery table
        and load into GCS Bucket in Json Format

 Parameters Required :-
    ProjectName - Your Project ID
    DatasetName - BigQuery Dataset Name
    Table - Table from where data is supposed to Export
    storageLocation - Destination Path (GCS Bucket e.g - gs://BUCKET NAME/DIR/)

    We need to explicitly set the tempLocation for beam runner to some temporary file

 */

public class test {
    // Setting up logger for printing console message or to debug

    private final static Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    // Assigning Environmental Variable

    public interface inputOption extends PipelineOptions {
         @Description("Setting Up Dataset Name")
         ValueProvider<String> getDataset();
        void setDataset(ValueProvider<String> value);

        @Description("Setting Up Table Name")
        ValueProvider<String> getTable();
        void setTable(ValueProvider<String> value);


        @Description("Setting Up Project ID")
        ValueProvider<String> getProjectName();
        void setProjectName(ValueProvider<String> value);

        @Description("GCS BUCKET LOCATION")
        ValueProvider<String> getStorageLocation();
        void setStorageLocation(ValueProvider<String> value);

    }

    public static void main(String[] args)
    {
        inputOption options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(inputOption.class);

        Pipeline pipeline = Pipeline.create(options);
        // Explicitly providing temp location (by default it is mandatory argument for
        //    reading from BigQueryIO

        options.setTempLocation("gs://dev-v2/temp");
        PCollection<TableRow> rows = pipeline
                .apply(
                        "Read from BigQuery query",
                        BigQueryIO.readTableRows()
                                .from(String.format("%s:%s.%s", options.getProjectName(), options.getDataset(), options.getTable()))

                );

 /*
      PCollection<TableRow> record = rows.apply(ParDo.of(new DoFn<TableRow, TableRow>() {
            @ProcessElement
            public void ProcessElement(ProcessContext c)
            {
                c.output(c.element());
            }
        }));

  */


        /* Converting PCollection<TableRow> into json format */
        PCollection<String> jsonForm = rows.apply("JsonTransform", AsJsons.of(TableRow.class));


        jsonForm.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void ProcessElement(ProcessContext c) {
                LOGGER.log(Level.INFO, (c.element()));

            }
        }));

        jsonForm.apply(TextIO.write().to(options.getStorageLocation()));
        PipelineResult result = pipeline.run();

    }


}
