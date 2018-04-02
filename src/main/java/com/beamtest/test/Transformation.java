package com.beamtest.test;

import ch.hsr.geohash.GeoHash;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by YaoWang on 2017/12/13.
 *
 * + Join both datasets in Beam
 * + Do the following transformation:
 *   - Filter any duplicate rows
 *   - string concatenating arrival and destination airport
 *   - geohashing the latitude and longitude
 *   - string padding the path_order with trailing zeros to a total string length of 10
 */
public class Transformation {
    static class ExtractFlightsFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String string = TransformationOperate(c.element());
            c.output(string);
        }
    }

    public static class TransformationAction extends PTransform<PCollection<String>,
            PCollection<String>> {
        @Override
        public PCollection<String> expand(PCollection<String> input) {
            PCollection<String> lines = input.apply(
                    ParDo.of(new ExtractFlightsFn()));

            return lines;
        }
    }

    public static class FormatAsTextFn extends SimpleFunction<String, String> {

        @Override
        public String apply(String input) {
            return input;
        }

    }

    public interface TransformationOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Required
        String getOutput();

        void setOutput(String value);
    }

    public static void main(String[] args) {
        TransformationOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(TransformationOptions.class);
        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.Read.from(options.getInputFile()))
                .apply(Distinct.<String>create()) // Filter any duplicate rows
                .apply(new TransformationAction())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteResult", TextIO.Write.to(options.getOutput()));

        p.run().waitUntilFinish();
    }

    /**
     * TransformationOperate
     *
     * @param string
     * @return
     */
    static String TransformationOperate(String string) {
        Pattern p = Pattern.compile(".*\\d+.*");
        Matcher m = p.matcher(string);
        if (!m.matches()) {
            string += ",GEOHASH_LATITUDE_LONGTITUDE";
        }

        // string concatenating arrival and destination(departure ?) airport
        List<String> list = new ArrayList<String>(Arrays.asList(string.split(",")));
        String str = list.get(3) + "-" + list.get(5);
        list.set(3, str);
        list.remove(5);

        // string padding the path_order with trailing zeros to a total string length of 10
        if (m.matches()) {
            list.set(list.size() - 1, list.get(list.size() - 1) + String.format("%1$0" + (10 - str.length()) + "d", 0));
            /*while (list.get(list.size() - 1).length() < 10) {
                list.set(list.size() - 1, list.get(list.size() - 1) + "0");
            }*/
        }

        StringBuilder stringBuilder = new StringBuilder();
        for (String s : list) {
            stringBuilder.append(s).append(",");
        }

        if (m.matches()) {
            // geohashing the latitude and longitude
            double lat = Double.parseDouble(list.get(list.size() - 3));
            double lon = Double.parseDouble(list.get(list.size() - 4));
            GeoHash geoHash = GeoHash.withCharacterPrecision(lat, lon, 12);
            stringBuilder.append(geoHash.toBase32());
        }


        return stringBuilder.substring(0, stringBuilder.length() - 1);
    }

}
