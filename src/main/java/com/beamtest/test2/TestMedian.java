package com.beamtest.test2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by YaoWang on 2017/12/16.
 *
 * To calculate the Median
 */
public class TestMedian {

    @SuppressWarnings("serial")
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from("flights_small.csv"))
                .apply("ExtractFightInfo", ParDo.of(new DoFn<String, KV<String, String>>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String[] words = c.element().split(",");

                        if (!words[0].equals("Date")) {
                            c.output(KV.of(words[1], words[0]));
                        }
                    }
                }))
                .apply("GroupByKey", GroupByKey.<String, String>create()) // create GroupByKey PTransform
                .apply("CalculateMedian", MapElements.via(
                        new SimpleFunction<KV<String, Iterable<String>>, String>() {

                            @Override
                            public String apply(KV<String, Iterable<String>> input) {
                                String airline = input.getKey();
                                Iterable<String> datesIt = input.getValue();
                                List<String> list = new ArrayList<>();
                                for (String str : datesIt) {
                                    list.add(str);
                                }
                                if (list.size() % 2 != 0) {
                                    return airline + " : " + list.get(list.size() / 2);
                                } else {
                                    //return airline + " : " + (list.get(list.size() / 2 - 1) + list.get(list.size() / 2)) / 2;
                                    return airline + " : " + list.get(list.size() / 2 - 1) + " " + list.get(list.size() / 2);
                                }
                            }
                        }))
                .apply("WriteResult", TextIO.write().to("MedianResult"));

        pipeline.run().waitUntilFinish();
    }
}
