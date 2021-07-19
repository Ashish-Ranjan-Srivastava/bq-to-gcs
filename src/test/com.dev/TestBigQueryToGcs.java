package com.dev;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link WordCount}. */
@RunWith(JUnit4.class)
public final class TestBigQueryToGcs {
    private static final String text = "{\"Name\":\"let\"}";
    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    @Category(NeedsRunner.class)
    public void testWordCountSimple() {

        PCollection<TableRow> rows = pipeline
                .apply(
                        "Read from BigQuery query",
                        BigQueryIO.readTableRows()
                                .from(String.format("%s:%s.%s", "gcp-training-poc", "test", "user"))

                );

        PCollection<String> jsonForm = rows.apply("JsonTransform", AsJsons.of(TableRow.class));
        PAssert.that(jsonForm).containsInAnyOrder(text);
        PipelineResult result = pipeline.run();
        result.waitUntilFinish();

    }
}
