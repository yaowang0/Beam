package com.beamtest.test2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Created by YaoWang on 2017/12/16.
 *
 * Join the weather and the Flights
 */
public class TestJoinWeatherFlights {

    @SuppressWarnings("serial")
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        // create flights info collection
        final PCollection<KV<String, String>> flightsInfoCollection = pipeline
                .apply(TextIO.read().from("flights_small.csv"))
                .apply("AirportDateFlightsPairs", MapElements.via(
                        new SimpleFunction<String, KV<String, String>>() {

                            @Override
                            public KV<String, String> apply(String input) {
                                // line format example: 2003/4/5,EV,20366,SHV,LA,MOB,AL,1820,-10,2048,30,2018,1830,-88.24,30.69,-88.24,30.69,AL to LA,1
                                String[] strings = input.split(",");
                                return KV.of(strings[3] + "-" + strings[0], input);
                            }
                        }));

        // create weather info collection
        final PCollection<KV<String, String>> weatherInfoCollection = pipeline
                .apply(TextIO.read().from("weather.csv"))
                .apply("AirportDateWeatherPairs", MapElements.via(
                        new SimpleFunction<String, KV<String, String>>() {

                            @Override
                            public KV<String, String> apply(String input) {
                                // line format example: 2006/3/14,ORD,2041,35,18.6
                                String[] strings = input.split(",");
                                return KV.of(strings[1] + "-" + strings[0], input);
                            }
                        }));

        final TupleTag<String> flightsInfoTag = new TupleTag<>();
        final TupleTag<String> weatherInfoTag = new TupleTag<>();

        final PCollection<KV<String, CoGbkResult>> cogrouppedCollection = KeyedPCollectionTuple
                .of(flightsInfoTag, flightsInfoCollection)
                .and(weatherInfoTag, weatherInfoCollection)
                .apply(CoGroupByKey.<String>create());

        final PCollection<KV<String, String>> finalResultCollection = cogrouppedCollection
                .apply("CreateJoinedIdInfoPairs", ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, CoGbkResult> kv = c.element();
                        String key = kv.getKey();
                        for (String flightsInfo : kv.getValue().getAll(flightsInfoTag)) {
                            for (String weatherInfo : kv.getValue().getAll(weatherInfoTag)) {
                                c.output(KV.of(key, "\t" + flightsInfo + "\t" + weatherInfo));
                            }
                        }

                    }
                }));

        PCollection<String> formattedResults = finalResultCollection
                .apply("FormatFinalResults", ParDo.of(new DoFn<KV<String, String>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(c.element().getKey() + c.element().getValue());
                    }
                }));

        formattedResults.apply(TextIO.write().to("JoinedResults"));
        pipeline.run().waitUntilFinish();

    }
}
