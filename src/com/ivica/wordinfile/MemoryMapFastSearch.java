package com.ivica.wordinfile;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author ikocev
 */
public class MemoryMapFastSearch {

    private static final String PATH = "enwik9";

    private static final int NUMBER_OF_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final String WORD = "Euler";
    private static final byte[] WORD_BYTES = WORD.getBytes();
    private static final int BUFFER_SIZE = 8192;

    private static ExecutorService executorService = Executors.newWorkStealingPool(NUMBER_OF_THREADS);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        for (int i = 0; i < 20; i++) {
            splitInChunksAndStartSearch(PATH, false);
            Thread.sleep(1000);
        }
        executorService.shutdown();
    }

    private static void splitInChunksAndStartSearch(final String pathFile, boolean enableLogging)
            throws InterruptedException, ExecutionException {
        Instant startInterval = Instant.now();

        long occurrences = 0;

        try (final RandomAccessFile file = new RandomAccessFile(pathFile, "r")) {
            FileChannel fileChannel = file.getChannel();

            long fileLength = fileChannel.size();
            if (enableLogging) {
                System.out.println("Total file length=" + fileLength);
            }

            long slices = fileLength / NUMBER_OF_THREADS;

            final List<CompletableFuture<Long>> threads = new ArrayList<>();
            for (int i = 0; i < NUMBER_OF_THREADS; i++) {
                final long startIndex = (i * slices);

                if (enableLogging) {
                    System.out.println("Spawning thread with start=" + (startIndex + WORD_BYTES.length) + " and length=" + slices);
                    System.out.println("Spawning thread with start=" + startIndex + " and length=" + slices);
                    System.out.println("===================================================");
                }
                final MappedByteBuffer mb = fileChannel.map(FileChannel.MapMode.READ_ONLY, startIndex, slices);

                threads.add(CompletableFuture.supplyAsync(() -> readFile(mb), executorService));
            }

            for (CompletableFuture<Long> thread : threads) {
                occurrences += thread.get();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Instant finish = Instant.now();
        System.out.println("Occurrences found=[" + occurrences + "] within " + Duration.between(startInterval, finish).toMillis() + " ms");
    }

    private static long readFile(final MappedByteBuffer mappedByteBuffer) {
        long counter = 0;

        byte[] bufferArray = new byte[BUFFER_SIZE];
        int bufferSize;

        while (mappedByteBuffer.hasRemaining()) {
            bufferSize = Math.min(mappedByteBuffer.remaining(), BUFFER_SIZE);

            mappedByteBuffer.get(bufferArray, 0, bufferSize);

            int matchIndex = 0;
            for (int i = 0; i < bufferSize; i++) {
                if (bufferArray[i] == WORD_BYTES[matchIndex]) {
                    matchIndex++;
                } else {
                    matchIndex = 0;
                }

                if (matchIndex == WORD_BYTES.length) {
                    counter++;
                    matchIndex = 0;
                }
            }
        }
        return counter;
    }
}
