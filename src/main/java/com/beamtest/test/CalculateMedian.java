package com.datatonic.test2;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by YaoWang on 2017/12/16.
 *
 * To calculate the median
 */
public class CalculateMedian {

    // read the FlightInfo
    static class ExtractFlightsInfoFnn extends DoFn<String, KV<String, String>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] words = c.element().split(",");

            if (!words[1].equals("Airline")) {
                c.output(KV.of(words[1], words[0]));
            }
        }
    }

    // calculate the Median
    static class CalMedian extends PTransform<PCollection<KV<String, String>>, PCollection<KV<String, String>>> {

        @Override
        public PCollection<KV<String, String>> expand(PCollection<KV<String, String>> kvpCollection) {
            PCollection<KV<String, String>> airlineDatePairs =
                    kvpCollection.apply((PTransform<? super PCollection<KV<String, String>>, PCollection<KV<String, String>>>) ParDo.of(new ExtractFlightsInfoFnn()));

            PCollection<KV<String, Iterable<String>>> airlineDates =
                    airlineDatePairs.apply(GroupByKey.<String, String>create());

            PCollection<KV<String, String>> results =
                    airlineDates.apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, KV<String, String>>() {
                        public void processElement(ProcessContext c){
                            String airline = c.element().getKey();
                            Iterable<String> datesIt = c.element().getValue();
                            List<String> list = new ArrayList<>();
                            for (String str : datesIt) {
                                list.add(str);
                            }
                            if (list.size() % 2 != 0) {
                                c.output(KV.of(airline, list.get(list.size() / 2)));
                            } else {
                                c.output(KV.of(airline, list.get(list.size() / 2 - 1) + list.get(list.size() / 2)));
                            }
                        }
                    }));
            return results;
        }
    }
}
