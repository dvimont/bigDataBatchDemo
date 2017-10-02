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
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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

/**
 * The file conversion provided by this class simply appends a 10-character 'yyyymmddhh'
 * String to the beginning of each WikiMedia PageViews record found in the original
 * files provided by WikiMedia. The year, month, day, and hour values are derived
 * from the name of the file in which the record is found.
 * @author Daniel Vimont
 */
public class WikiMediaFileConsolidator {
    public static void consolidateFiles (String sourceFileName, String targetPathString)
            throws IOException {
        consolidateFiles(sourceFileName, targetPathString, false, true, 0);
    }

    /**
     * This method simply appends a 10-character 'yyyymmddhh'
     * String to the beginning of each WikiMedia PageViews record found in the original
     * files provided by WikiMedia. The year, month, day, and hour values are derived
     * from the name of the file in which the record is found.
     *
     * @param sourceFileName
     * @param targetPathString
     * @param debugMode
     * @param verboseMode
     * @param processingLimit Note that processingLimit applies to each file being converted,
     * so when processing N files, the total records processed will be (N * processingLimit).
     *
     * @throws IOException
     */
    public static void consolidateFiles (String sourceFileName, String targetPathString,
            boolean debugMode, boolean verboseMode, int processingLimit)
            throws IOException {
        System.out.println("*** Formatting/consolidation of WikiMedia files commencing at: " + LocalDateTime.now());
        int processedRecordCount = 0;
        int badRecordCount = 0;
        Configuration conf = new Configuration(); // fetch Hadoop config parms
        try (FileSystem fileSys = FileSystem.get(conf)) { // fileSys could be HDFS, or S3, etc.
            Path targetFile = new Path(targetPathString);
            if ( fileSys.exists(targetFile)) {
                fileSys.delete(targetFile, true);
            }
            try ( OutputStream outputStream = fileSys.create(targetFile, () -> { System.out.print("."); });
                    BufferedWriter bufferedWriter =
                            new BufferedWriter(new OutputStreamWriter( outputStream, "UTF-8" )) ) {

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

                int maxDomainCodeLength = 0;
                String maxDomainCodeLengthExample = "";

                for (String sourceFile : fileList) {
                    System.out.println("** Formatting/consolidating raw data from file: <" + sourceFile + ">");
                    String yearMonthDayHour = sourceFile.substring(10, 18) + sourceFile.substring(19, 21);

                    try (GZIPInputStream unzipper = new GZIPInputStream(new FileInputStream(
                            WikimediaFileDownloader.WORKSPACE_PATH_PREFIX_STRING + sourceFile));
                            BufferedReader reader = new BufferedReader(new InputStreamReader(unzipper)) ) {
                        String rawLine;
                        int processingLineCount = 0;
                        StringBuilder stringBuilder = new StringBuilder();
                        boolean pastFirstLine = false;
                        while ((rawLine = reader.readLine()) != null) {
                            processedRecordCount++;
                            String[] rawLineComponents = rawLine.split(" ");
                            if (rawLineComponents.length == 4) {
                                String domainCode = rawLineComponents[0];
                                if (domainCode.length() > maxDomainCodeLength) {
                                    maxDomainCodeLength = domainCode.length();
                                    maxDomainCodeLengthExample = domainCode;
                                }
                            } else {
                                badRecordCount++;
                                continue;
                            }

                            stringBuilder.setLength(0);
                            stringBuilder.append(yearMonthDayHour).append(rawLine);
                            if (pastFirstLine) {
                                bufferedWriter.newLine();
                            } else {
                                pastFirstLine = true;
                            }
                            bufferedWriter.write(stringBuilder.toString());

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
                        "Maximum DomainCode length: " + maxDomainCodeLength + "\n" +
                        "Maximum DomainCode length example: " + maxDomainCodeLengthExample + "\n" +
                        "Bad records bypassed: " + badRecordCount
                );
            }
        }
        System.out.println("*** Formatting/consolidation of WikiMedia files completed at: " + LocalDateTime.now());
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

}
