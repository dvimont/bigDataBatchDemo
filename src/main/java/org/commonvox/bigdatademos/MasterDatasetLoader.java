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

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.commonvox.bigdatademos.PageViewsPerHourProto.PageViewsPerHour;
/**
 *
 * @author Daniel Vimont
 */
public class MasterDatasetLoader {
    private static final int RECORD_TERMINATOR = 0;

    public static void doETL (String sourceFileName, String targetPathString)
            throws IOException {
        doETL(sourceFileName, targetPathString, false, true, 0);
    }

    public static void doETL (String sourceFileName, String targetPathString,
            boolean debugMode, boolean verboseMode, int processingLimit)
            throws IOException {
        System.out.println("*** Loading of Master Dataset commencing at: " + LocalDateTime.now());
        int processedRecordCount = 0;
        int badRecordCount = 0;
        Configuration conf = new Configuration(); // fetch Hadoop config parms
        try (FileSystem fileSys = FileSystem.get(conf)) { // fileSys could be HDFS, or S3, etc.
            Path targetFile = new Path(targetPathString);
            if ( fileSys.exists(targetFile)) {
                fileSys.delete(targetFile, true);
            }
            try ( OutputStream outputStream = fileSys.create(targetFile, () -> { System.out.print("."); });
                    BufferedOutputStream bufferedOutput = new BufferedOutputStream(outputStream) ) {

                Set<String> fileList;
                if (sourceFileName.equals("*")) {
                    fileList = getFileList(WikimediaFileDownloader.WORKSPACE_PATH_PREFIX_STRING);
                    System.out.println("*** LIST of files to be processed.");
                    for (String sourceFile : fileList) {
                        System.out.println(sourceFile);
                    }
                    System.out.println("*** END OF LIST of files to be processed.");
                } else {
                    fileList = new TreeSet<>();
                    fileList.add(sourceFileName);
                }

                for (String sourceFile : fileList) {
                    System.out.println("** Importing raw data into MasterDataset from file: <" + sourceFile + ">");
                    int year = Integer.parseInt(sourceFile.substring(10, 14));
                    int month = Integer.parseInt(sourceFile.substring(14, 16));
                    int day = Integer.parseInt(sourceFile.substring(16, 18));

                    try (GZIPInputStream unzipper = new GZIPInputStream(new FileInputStream(
                            WikimediaFileDownloader.WORKSPACE_PATH_PREFIX_STRING + sourceFile));
                            BufferedReader reader = new BufferedReader(new InputStreamReader(unzipper)) ) {
                        String rawLine;
                        int processingLineCount = 0;
                        while ((rawLine = reader.readLine()) != null) {
                            processedRecordCount++;
                            PageViewsPerHour pageViewsObject =
                                    buildProtoBufEntry(year, month, day, rawLine, verboseMode);
                            if (pageViewsObject == null) { // skip over invalid raw data entries
                                badRecordCount++;
                                continue;
                            }
                            byte[] pageviewsByteArray = pageViewsObject.toByteArray();
                            if (debugMode) {
                                doDebugProcessing(rawLine, pageViewsObject, pageviewsByteArray);
                            }
                            bufferedOutput.write(pageviewsByteArray);
                            bufferedOutput.write(RECORD_TERMINATOR);
                            if (processingLimit > 0 && ++processingLineCount >= processingLimit) {
                                break;
                            }
                        }
                    }
                }
                System.out.println("============\n" +
                        "Loading of Master Dataset complete.\n" +
                        "Files processed: " + fileList.size() + "\n" +
                        "Records processed: " + processedRecordCount + "\n" +
                        "Bad records bypassed: " + badRecordCount);
            }
        }
        System.out.println("*** Loading of Master Dataset completed at: " + LocalDateTime.now());
    }

    private static Set<String> getFileList(String directory) {
        Set<String> fileNames = new TreeSet<>();
        try (DirectoryStream<java.nio.file.Path> directoryStream = Files.newDirectoryStream(Paths.get(directory))) {
            for (java.nio.file.Path path : directoryStream) {
                fileNames.add(path.toFile().getName());
            }
        } catch (IOException ex) {}
        return fileNames;
    }


    private static PageViewsPerHour buildProtoBufEntry (int year, int month, int day, String rawDataEntry,
            boolean verboseMode) {
        String[] parsedData = rawDataEntry.split(" ");
        if (parsedData.length != 4) {
            if (verboseMode) {
                System.out.println("** Encountered raw-data line in unexpected format: <" + rawDataEntry + ">");
            }
            return null;
        }
        return PageViewsPerHour.newBuilder()
                        .setHourlyTimestamp(
                                PageViewsPerHour.HourlyTimestamp.newBuilder()
                                        .setYear(year).setMonth(month).setDay(day))
                        .setDomainCode(parsedData[0])
                        .setPageTitle(parsedData[1])
                        .setCountViews(Integer.parseInt(parsedData[2]))
                        .setTotalResponseSize(Integer.parseInt(parsedData[3]))
                .build();
    }

    private static void doDebugProcessing(String rawLine,
            PageViewsPerHour pageViewsObject, byte[] pageviewsByteArray)
            throws InvalidProtocolBufferException {
        StringBuffer errorMsg = new StringBuffer();
        boolean errorFound = false;
        PageViewsPerHour deserializedPageViews =
                PageViewsPerHour.parseFrom(pageviewsByteArray);
        if (!pageViewsObject.getDomainCode().equals(deserializedPageViews.getDomainCode())) {
            errorFound = true;
            errorMsg.append("** DomainCode mangled: original == <" +
                    pageViewsObject.getDomainCode() + ">, serialized/deserialized == <" +
                    deserializedPageViews.getDomainCode() + ">\n");
        }
        if (!pageViewsObject.getPageTitle().equals(deserializedPageViews.getPageTitle())) {
            errorFound = true;
            errorMsg.append("** PageTitle mangled: original == <" +
                    pageViewsObject.getPageTitle() + ">, serialized/deserialized == <" +
                    deserializedPageViews.getPageTitle() + ">\n");
        }
        if (pageViewsObject.getCountViews() != deserializedPageViews.getCountViews()) {
            errorFound = true;
            errorMsg.append("** CountViews mangled: original == <" +
                    pageViewsObject.getCountViews() + ">, serialized/deserialized == <" +
                    deserializedPageViews.getCountViews() + ">\n");
        }
        if (pageViewsObject.getTotalResponseSize() != deserializedPageViews.getTotalResponseSize()) {
            errorFound = true;
            errorMsg.append("** TotalResponseSize mangled: original == <" +
                    pageViewsObject.getTotalResponseSize() + ">, serialized/deserialized == <" +
                    deserializedPageViews.getTotalResponseSize() + ">\n");
        }
        if (errorFound) {
            System.out.println("++ MANGLING DETECTED IN PROTOBUF SERIALIZATION PROCESS!!");
            System.out.println("  --- Original line read in from WikiMedia file: <" + rawLine + ">");
            System.out.println("  --- COMPLETE ORIGINAL PAGEVIEWS OBJECT...");
            System.out.println(pageViewsObject);
            System.out.print(errorMsg);
        }
    }

    protected static void readMasterDataset(String pathString, int processingLimit)
            throws FileNotFoundException, IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        try ( BufferedInputStream reader = new BufferedInputStream(new FileInputStream(pathString)) ) {
            int nextInt;
            int lineCount = 0;
            while ((nextInt = reader.read()) != -1) {
                byte nextByte = (byte)nextInt;

                if (nextInt == RECORD_TERMINATOR) {
                    line.flush();
                    System.out.println(PageViewsPerHour.parseFrom(line.toByteArray()));
                    line.reset();

                    if (++lineCount >= processingLimit) {
                        break;
                    }
                } else {
                    line.write(nextInt);
                }
            }
            System.out.println("=========\nLines read: " + lineCount);
        }
    }

    private static void displayByteArray(byte[] array) {
        StringBuilder stringBuilder = new StringBuilder();
        for (byte nextByte : array) {
            stringBuilder.append(String.format("%02x", nextByte));
        }
        System.out.println(stringBuilder.toString());
    }
}
