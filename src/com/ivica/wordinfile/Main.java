package com.ivica.wordinfile;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

public class Main {

    private static final String PATH = "enwik9";

    private static final int NUMBER_OF_THREADS = 16;
    private static final String WORD = "Euler";
    private static final int OFFSET = WORD.length() - 1;
    private static final byte[] W_BYTES = WORD.getBytes();
    private static final int SIZE = 8192;

//    static ExecutorService executor = new ThreadPoolExecutor(NUMBER_OF_THREADS, NUMBER_OF_THREADS,
//            10_000, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
    static ExecutorService executor = Executors.newFixedThreadPool(16);


    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        findWordInFile();
        //slowFind();
    }

    private static void slowFind() throws IOException {
        FileInputStream inputStream = null;
        Scanner sc = null;
        int occurrences = 0;
        try {
            inputStream = new FileInputStream(PATH);
            sc = new Scanner(inputStream, "UTF-8");
            while (sc.hasNextLine()) {
                int start = 0;
                String line = sc.nextLine();
                while (start < line.length() && line.indexOf(WORD, start) != -1) {
                    occurrences++;
                    start = line.indexOf(WORD, start) + WORD.length();
                }
            }
            // note that Scanner suppresses exceptions
            if (sc.ioException() != null) {
                throw sc.ioException();
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (sc != null) {
                sc.close();
            }
        }
        System.out.println("Total occurrences = " + occurrences);
    }


    private static void findWordInFile() throws InterruptedException, ExecutionException {
        Instant startInterval = Instant.now();

        long occurrences = 0;
        long totalLength = 0;

        try(final RandomAccessFile file = new RandomAccessFile(PATH, "r")) {
            totalLength = file.getChannel().size();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Total file length=" + totalLength);

        long slices = totalLength / NUMBER_OF_THREADS;

        final List<CompletableFuture<Long>> threads = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            long startIndex =  i * slices;
            System.out.println("Spawning thread with start=" + startIndex + " and length=" + slices);
            threads.add(CompletableFuture.supplyAsync(() -> readFile(startIndex, slices), executor));
        }

        for (CompletableFuture<Long> thread : threads) {
            occurrences += thread.get();
        }

        Instant finish = Instant.now();
        System.out.println("Occurrences found=[" + occurrences + "]");
        System.out.println(Duration.between(startInterval, finish).toMillis() + " ms");

        executor.shutdown();
    }


    private static long readFile(final long start, final long length) {
        long counter = 0;

        try(final RandomAccessFile file = new RandomAccessFile(PATH, "r")) {
            final MappedByteBuffer mb = file.getChannel().map(FileChannel.MapMode.READ_ONLY, start, length);

            byte[] bufferArray = new byte[SIZE];
            int nGet;
            while(mb.hasRemaining()) {
                nGet = Math.min(mb.remaining(), SIZE);

                mb.get(bufferArray, 0, nGet);

                int matchIndex = 0;
                for (int i = 0; i < nGet; i++) {

                    if (bufferArray[i] == W_BYTES[matchIndex]) {
                        matchIndex++;
                    } else {
                        matchIndex = 0;
                    }

                    if (matchIndex == W_BYTES.length) {
                        counter++;
                        matchIndex = 0;
                    }
                }
            }
        }  catch (IOException e) {
            e.printStackTrace();
        }

        return counter;
    }
}
