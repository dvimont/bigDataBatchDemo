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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author Daniel Vimont
 */
public class WikimediaFileDownloader {
    public static final String WIKIMEDIA_PAGEVIEW_FILES_DIRECTORY_URL =
            "https://dumps.wikimedia.org/other/pageviews/";
    public static final String DIRECTORY_2015_EXTENSION = "2015/";
    public static final String DIRECTORY_2016_EXTENSION = "2016/";
    public static final String DIRECTORY_2017_EXTENSION = "2017/";
    public static final String WORKSPACE_PATH_PREFIX_STRING = "./workspace/";

    public static final String FIXED_SAMPLE = "fixed_sample";
    public static final String[] FIXED_SAMPLE_FILES =
        {
            // files from different hours in same day
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-07/pageviews-20160701-110000.gz",
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-07/pageviews-20160701-140000.gz",
            // files from same week as above, different day
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-07/pageviews-20160702-110000.gz",
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-07/pageviews-20160702-140000.gz",
            // files from same month as above, different week
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-07/pageviews-20160729-110000.gz",
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-07/pageviews-20160729-140000.gz",
            // files from same year as above, different month
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-09/pageviews-20160929-110000.gz",
            "https://dumps.wikimedia.org/other/pageviews/2016/2016-09/pageviews-20160929-140000.gz",
        };
    public static final String HDFS_RAW_DATA_DIRECTORY = "/test/raw_files/";

    public static void main( String[] args ) throws Exception {
        System.out.println("YEP");

        String filePath = null;
        int processingLimit = 0;
        if (args.length > 0) {
            if (args[0].toLowerCase().equals("sample")) {
                filePath = WikimediaFileDownloader.FIXED_SAMPLE;
            } else {
                filePath = args[0];
                if (args.length > 1) {
                    processingLimit = Integer.valueOf(args[1]);
                }
            }
        }
        downloadRawDataFiles(filePath, processingLimit);
    }    
    
    public static void downloadRawDataFiles(String urlString, int processingLimit)
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        if (urlString == null) {
            urlString = WIKIMEDIA_PAGEVIEW_FILES_DIRECTORY_URL;
        }
        TreeSet<String> pageviewFileUrlStrings;
        if (urlString.equals(FIXED_SAMPLE)) {
            System.out.println("** Raw data files downloaded based upon fixed list!");
            pageviewFileUrlStrings = new TreeSet<>();
            pageviewFileUrlStrings.addAll(Arrays.asList(FIXED_SAMPLE_FILES));
        } else {
            System.out.println("** Recursively processing WikiMedia index pages to derive "
                    + "list of downloadable raw data files!");
            pageviewFileUrlStrings = getPageviewFileUrlStrings(urlString);
        }
        
//        //Get configuration of Hadoop system
//        Configuration conf = new Configuration();
//        System.out.println("Connecting to -- " + conf.get("fs.defaultFS") +
//                " for copying downloaded files to HDFS.");

        System.out.println("=================");
        System.out.println("Number of pageview files on remote server == " + pageviewFileUrlStrings.size());
        System.out.println("** Commencing remote copy of WikiMedia raw data files with processingLimit == " + processingLimit);
        int processingCount = 0;
        for (String pageviewFileUrlString : pageviewFileUrlStrings) {
            System.out.println("Copying file from URL: " + pageviewFileUrlString);
            Path newLocalFile = copyRemoteFile(pageviewFileUrlString);
//            System.out.println("Copying downloaded file into HDFS");
//            copyLocalFileToHDFS(newLocalFile, conf);
//            System.out.println("Deleting originally downloaded file");
//            Files.delete(newLocalFile);
            if (processingLimit > 0 && ++processingCount >= processingLimit) {
                break;
            }
        }
    }

    /**
     * Note that the directory pages for Wikimedia pageview data files are
     * indexed in a hierarchical file system of html pages, which this method
     * recursively traverses until it finds the links to all pageview files.
     *
     * @param indexPageUrlString
     * @return Set of URL Strings of all pageview statistics files at WikiMedia
     *
     * @throws java.security.NoSuchAlgorithmException
     * @throws java.security.KeyManagementException
     * @throws java.io.IOException
     */
    private static TreeSet<String> getPageviewFileUrlStrings(String indexPageUrlString)
            throws NoSuchAlgorithmException, KeyManagementException, IOException {

        TreeSet<String> pageviewFileUrlStrings = new TreeSet<>();

        String indexPageContent = getIndexPageContent(indexPageUrlString);
        // (extract all 'a' tags with 'href' attribute)
        Elements links = Jsoup.parse(indexPageContent).select("a[href]"); 
        for (Element link : links) {
            String href = link.attr("href");
            if (href.matches("201\\d.*/")) {
                System.out.println(indexPageUrlString + href);
                pageviewFileUrlStrings.addAll(getPageviewFileUrlStrings(indexPageUrlString + href));
            } else if (href.matches("pageviews-201.*\\.gz")) {
                pageviewFileUrlStrings.add(indexPageUrlString + href);
            }
        }
        return pageviewFileUrlStrings;
    }

    private static String getIndexPageContent(String indexPageUrlString)
            throws MalformedURLException, NoSuchAlgorithmException, KeyManagementException, IOException {
        HttpsURLConnection conn = getConnectionToServerWithBadSslCertificate(new URL(indexPageUrlString));
        try ( BufferedReader buffer = new BufferedReader(new InputStreamReader(conn.getInputStream())) ) {
            return buffer.lines().collect(Collectors.joining("\n"));
        }
    }

    private static Path copyRemoteFile(String urlString)
            throws IOException, NoSuchAlgorithmException, KeyManagementException {
        URL url = new URL(urlString);
        Path targetPath = Paths.get(
                WORKSPACE_PATH_PREFIX_STRING + Paths.get(urlString).getFileName().toString());

        HttpsURLConnection conn = getConnectionToServerWithBadSslCertificate(url);
        try ( InputStream inputStream = conn.getInputStream() )
        {
            Files.copy(inputStream, targetPath, REPLACE_EXISTING);
        }
        return targetPath;
    }

//    private static void copyLocalFileToHDFS(Path localFilePath, Configuration conf)
//            throws FileNotFoundException, IOException {
//        //Input stream for the file in local file system to be written to HDFS
//        InputStream in =
//                new BufferedInputStream(new FileInputStream(localFilePath.toFile()));
//        //Destination file in HDFS
//        String targetHdfsFile = HDFS_RAW_DATA_DIRECTORY + localFilePath.getFileName();
//        FileSystem fs = FileSystem.get(URI.create(targetHdfsFile), conf);
//        org.apache.hadoop.fs.Path targetHdfsPath =
//                new org.apache.hadoop.fs.Path(targetHdfsFile);
//        // Set outputStream with 64MB blocks (Wikimedia files < 64MB)
//        OutputStream outStream = fs.create(targetHdfsPath, true, 4096,
//                (short)conf.getInt("dfs.replication", 3), 67108864);
//        //Copy file from local to HDFS
//        IOUtils.copyBytes(in, outStream, 4096, true);
//        
//        // NOTE that #moveFromLocalFile does not seem to allow control over blockSize
//        //   fs.moveFromLocalFile( 
//        //              new org.apache.hadoop.fs.Path(localFilePath.toUri()), targetHdfsPath);
//    }
//
    private static HttpsURLConnection getConnectionToServerWithBadSslCertificate(URL url)
            throws NoSuchAlgorithmException, KeyManagementException,
            MalformedURLException, IOException {

        // Dummy TrustManager to bypass expired SSL certificate (Hey kids, don't try this at home!!)
        X509TrustManager tm = new X509TrustManager() {
            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }
            @Override
            public void checkServerTrusted(X509Certificate[] paramArrayOfX509Certificate, String paramString) {
                // do nothing
            }
            @Override
            public void checkClientTrusted(X509Certificate[] paramArrayOfX509Certificate, String paramString) {
                // do nothing
            }
        };

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, new TrustManager[] { tm }, null);
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
        // We don't want to bypass SSL validation for all processes in JVM session,
        //   so get new SSL socket factory just for this instance!!
        conn.setSSLSocketFactory(ctx.getSocketFactory());
        conn.setHostnameVerifier((String paramString, SSLSession paramSSLSession) -> true);
        // the above lambda expression derived from the old-school anonymous class coding as follows:
        //        conn.setHostnameVerifier(new HostnameVerifier() {
        //            @Override
        //            public boolean verify(String paramString, SSLSession paramSSLSession) {
        //                return true;
        //            }
        //        });
        conn.connect();
        return conn;
    }

//    // WOW!! WikiMedia's SSL certificate is apparently expired. Invoking the following
//    //  results in a javax.net.ssl.SSLHandshakeException being thrown!
//    //  Workaround for this found here:
//    //    https://javaskeleton.blogspot.com/2011/01/avoiding-sunsecurityvalidatorvalidatore.html
//    private void copyRemoteFileThrowsSslException() throws IOException {
//        String urlString = WIKIMEDIA_PAGEVIEW_FILES_DIRECTORY_URL;
//        Path targetPath = Paths.get(WORKSPACE_PATH_PREFIX_STRING + Paths.get(urlString).getFileName().toString());
//
//        URL url = new URL(urlString);
//        try ( InputStream inputStream = url.openStream() )
//        {
//            Files.copy(inputStream, targetPath);
//        }
//    }
    
}
