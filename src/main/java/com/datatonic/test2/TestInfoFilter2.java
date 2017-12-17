package com.datatonic.test2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

/**
 * Created by YaoWang on 2017/12/16.
 *
 * To deduplicate the flights only based on date, airline_code and route.
 */
public class TestInfoFilter2 {

    // read the text
    @SuppressWarnings("serial")
    static class ExtractFlightsFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element());
        }
    }

    // deduplicate the flights only based on date, airline_code and route
    @SuppressWarnings("serial")
    public static class DistinctTransform extends PTransform<PCollection<String>,
            PCollection<String>> {
        @Override
        public PCollection<String> expand(PCollection<String> input) {

            PCollection<String> lines = input.apply(
                    ParDo.of(new ExtractFlightsFn()));

            PCollection<String> distinctData = lines.apply(Distinct.withRepresentativeValueFn(new DistinctValues()));

            return distinctData;
        }
    }

    // define the distinct function
    @SuppressWarnings("serial")
    public static class DistinctValues implements SerializableFunction<String, String> {
        /*
         * s.split(",")[0] is the date.
         * s.split(",")[2] is the airline_code,
         * s.split(",")[s.split(",").length - 2] is the route
         */
        @Override
        public String apply(String s) {
            return s.split(",")[0] + s.split(",")[2] + s.split(",")[s.split(",").length - 2];
        }
    }

    // output Format
    @SuppressWarnings("serial")
    public static class FormatAsTextFn extends SimpleFunction<String, String> {
        @Override
        public String apply(String input) {
            return input;
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadLines", TextIO.Read.from("flights_small.csv"))
                .apply(new DistinctTransform())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteResult", TextIO.Write.to("DeduplicateResult"));

        pipeline.run().waitUntilFinish();
    }
}
