/*
 * Copyright 2017 Daniel Vimont.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commonvox.bigdatademos;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.zip.GZIPInputStream;
import org.commonvox.bigdatademos.PageViewsPerHourProto.PageViewsPerHour;

//import org.commonvox.bigdatademos.AddressBookProtos.Person;
//import org.commonvox.bigdatademos.PageViewsPerHour;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {
        System.out.println( "Hello World!" );
//        new App().buildProtobufExample();

//        downloadSampleMultiyearRawDataFiles(3);

//        MasterDatasetLoader.doETL("*",   // "pageviews-20170101-000000.gz",
//                "/home/dv/hdfsData/pageviews/pageviews.data", true, false, 0);
//        MasterDatasetLoader.readMasterDataset("/home/dv/hdfsData/pageviews/pageviews.data", 10);

        WikimediaFileDownloader.downloadRawDataFiles(WikimediaFileDownloader.FIXED_SAMPLE, 0);


        // NOTE that WikiMediaFileConsolidator is no longer used!
//        WikiMediaFileConsolidator.consolidateFiles(
//                "*", "/home/dv/hdfsData/pageviews/pageviews.hourly.txt", true, true, 0);

    }

    public static void downloadSampleMultiyearRawDataFiles(int processingLimit)
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        WikimediaFileDownloader.downloadRawDataFiles(
                WikimediaFileDownloader.WIKIMEDIA_PAGEVIEW_FILES_DIRECTORY_URL +
                        WikimediaFileDownloader.DIRECTORY_2015_EXTENSION,
                processingLimit);
        WikimediaFileDownloader.downloadRawDataFiles(
                WikimediaFileDownloader.WIKIMEDIA_PAGEVIEW_FILES_DIRECTORY_URL +
                        WikimediaFileDownloader.DIRECTORY_2016_EXTENSION,
                processingLimit);
        WikimediaFileDownloader.downloadRawDataFiles(
                WikimediaFileDownloader.WIKIMEDIA_PAGEVIEW_FILES_DIRECTORY_URL +
                        WikimediaFileDownloader.DIRECTORY_2017_EXTENSION,
                processingLimit);
    }

//    public static void testSundayCalculator() {
//        LocalDate localDate = LocalDate.of(2015, 5, 1);
//        int sundayOffset = localDate.getDayOfWeek().getValue() % 7;
//        System.out.println("Nearest preceding Sunday was: " + localDate.minusDays(sundayOffset).toString());
//    }

    public static void readGzippedFile(String pathString, int processingLimit)
            throws FileNotFoundException, IOException {
        try ( GZIPInputStream unzipper = new GZIPInputStream(new FileInputStream(pathString));
                BufferedReader reader = new BufferedReader(new InputStreamReader(unzipper)) ) {
            String line;
            int lineCount = 0;
            while ((line = reader.readLine()) != null) {
                if (++lineCount >= processingLimit) {
                    break;
                }
                System.out.println(line);
            }
            System.out.println("=========\nLines read: " + lineCount);
        }
    }

    public void buildProtobufExample() {
        // example for serializing "en Electron_counting 3 0"
        PageViewsPerHour entry =
                PageViewsPerHour.newBuilder()
                        .setHourlyTimestamp(
                                PageViewsPerHour.HourlyTimestamp.newBuilder()
                                        .setYear(2015).setMonth(11).setDay(17))
                        .setDomainCode("en")
                        .setPageTitle("Electron_counting")
                        .setCountViews(3)
                        .setTotalResponseSize(0)
                .build();
        System.out.println("Here is the entry content:");
        System.out.println(entry);
    }
}
