package com.beamtest.test;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import javax.naming.ldap.SortKey;
import java.util.List;

/**
 * Created by YaoWang on 2017/12/13.
 *
 * + Calculation2 the following aggregates for every airline:
 *   - Total flights
 *   - Total flights per day
 */
public class Calculation2 {

    static class ExtractFlightsFn extends DoFn<String, KV<String, Integer>> {

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
                c.output(KV.of(words[1] + " " + words[0], 1));
            }
        }
    }

    public static class CountFlights extends PTransform<PCollection<KV<String, Integer>>,
            PCollection<KV<String, Integer>>> {
        @Override
        public PCollection<KV<String, Integer>> expand(PCollection<KV<String, Integer>> lines) {

            PCollection<KV<String, Integer>> flight =
                    lines.apply((PTransform<? super PCollection<KV<String, Integer>>, PCollection<KV<String, Integer>>>) ParDo.of(new ExtractFlightsFn()));

            // count
//            PCollection<KV<String, Long>> totalFlights =
//                    flight.apply(Count.<String>perElement());

            /*
             * Calculate the median
             */
            PCollection<KV<String, Iterable<Integer>>> calMed =
                    flight.apply(GroupByKey.<String, Integer>create());
/*          PCollection<KV<String, Doc>> urlDocPairs = ...;
            PCollection<KV<String, Iterable<Doc>>> urlToDocs =
                    urlDocPairs.apply(GroupByKey.<String, Doc>create());
            PCollection<R> results =
                    urlToDocs.apply(ParDo.of(new DoFn<KV<String, Iterable<Doc>>, R>() {
                        public void processElement(ProcessContext c) {
                            String url = c.element().getKey();
                            Iterable<Doc> docsWithThatUrl = c.element().getValue();
         ... process all docs having that url ...
                        }}));
*/

            return null;
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
//                .apply(new CountFlights())
//                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteResult", TextIO.Write.to(options.getOutput()));

        p.run().waitUntilFinish();
    }
}
