package com.dev;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.util.Arrays;
import java.util.List;

import static com.dev.BigQueryToGcs.ParseIntoJson;



@RunWith(JUnit4.class)
public final class TestBigQueryToGcs
{
    private static final String text = "{\"Name\":\"let\"}";
   @Rule public final transient TestPipeline pipeline = TestPipeline.create();


    @Test
    @Category(NeedsRunner.class)
    public void testBqToGcs() {

        
        // create known TableRow record which can be parsed into json later
        
        final List<KV<String, String>> record =
                Arrays.asList(
                        KV.of("Tom", "Active"));
        PCollection<KV<String, String>> data = pipeline.apply( Create.of(record));

        PCollection<TableRow> value = data.apply(ParDo.of(new DoFn<KV<String, String>, TableRow>() {
            @ProcessElement
            public void ProcessElement(ProcessContext c)
            {
                TableRow row = new TableRow()
                        .set("Name", c.element().getKey())
                        .set("state", c.element().getValue());
                c.output(row);
            }
        }));

        // testing conversion of json 
        
        // calling ParseIntoJson function from BigqueryToGcs class (driver class)
        PCollection<String> jsonForm = ParseIntoJson(value);
        PAssert.that(jsonForm).containsInAnyOrder(text);
        PipelineResult result = pipeline.run();
        result.waitUntilFinish();

    }
}
