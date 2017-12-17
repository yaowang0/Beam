package com.datatonic.test;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Created by YaoWang on 2017/12/13.
 *
 * + Calculation the following aggregates for every airline:
 *   - Total flights
 *   - Total flights per day
 */
public class Calculation {

    static class ExtractFlightsFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            // split the line
            String[] words = c.element().split(",");

            /*// Total flights
            if (!words[1].equals("Airline")) {
                c.output(words[1]);
            }*/

            // Total flights per day
            if (!words[1].equals("Airline")) {
                c.output(words[1] + " " + words[0]);
            }
        }
    }

    public static class CountFlights extends PTransform<PCollection<String>,
            PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            PCollection<String> flight = lines.apply(
                    ParDo.of(new ExtractFlightsFn()));

            // count
            PCollection<KV<String, Long>> totalFlights =
                    flight.apply(Count.<String>perElement());

            return totalFlights;
        }
    }

    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    public interface CountFlightsOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Required
        String getOutput();
        void setOutput(String value);
    }

    public static void main(String[] args) {
        CountFlightsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(CountFlightsOptions.class);
        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.Read.from(options.getInputFile()))
                .apply(Distinct.<String>create()) // Filter any duplicate rows
                .apply(new CountFlights())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteResult", TextIO.Write.to(options.getOutput()));

        p.run().waitUntilFinish();
    }
}
