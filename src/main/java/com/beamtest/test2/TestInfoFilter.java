package com.beamtest.test2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/**
 * Created by YaoWang on 2017/12/16.
 *
 * To deduplicate the flights only based on date, airline_code and route.
 */
public class TestInfoFilter {

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
            PCollection<String> inputFlightInfo =
                    input.apply(ParDo.of(new ExtractFlightsFn()));
            PCollection<String> outputFlightInfo = inputFlightInfo.apply(Distinct.withRepresentativeValueFn(new DistinctValues()));
            return outputFlightInfo;
        }
    }

    // define the distinct function
    @SuppressWarnings("serial")
    public static class DistinctValues implements SerializableFunction<String, String> {

        @Override
        public String apply(String string) {
            FlightInfo flightInfo = string2FlightInfo(string);
            return flightInfo.toString();
        }
    }

    // output format
    @SuppressWarnings("serial")
    public static class FormatAsTextFn extends SimpleFunction<String, String> {

        @Override
        public String apply(String input) {
            return input;
        }

    }

    @SuppressWarnings("serial")
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadLines", TextIO.read().from("flights_small.csv"))
                .apply(new DistinctTransform())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteResult", TextIO.write().to("DeduplicateResult"));

        pipeline.run().waitUntilFinish();
    }

    static FlightInfo string2FlightInfo(String string) {
        String[] strings = string.split(",");
        /*
         * strings[0] is the date
         * strings[2] is the airline_code
         * strings[strings.length - 2] is the route.
         */
        return new FlightInfo(strings[0], strings[2], strings[strings.length - 2]);
    }
}
