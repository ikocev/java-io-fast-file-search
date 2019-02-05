package com.ivica.wordinfile;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Main {

    private static final String PATH = "enwik9";

    private static final int NUMBER_OF_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final String WORD = "Euler";
    private static final byte[] WORD_BYTES = WORD.getBytes();
    private static final int BUFFER_SIZE = 8192;

    private static ExecutorService executorService = Executors.newWorkStealingPool(NUMBER_OF_THREADS);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        splitInChunksAndStartSearch();
    }

    private static void splitInChunksAndStartSearch() throws InterruptedException, ExecutionException {
        Instant startInterval = Instant.now();

        long occurrences = 0;
        long fileLength = 0;

        try(final RandomAccessFile file = new RandomAccessFile(PATH, "r")) {
            fileLength = file.getChannel().size();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Total file length=" + fileLength);

        long slices = fileLength / NUMBER_OF_THREADS;

        final List<CompletableFuture<Long>> threads = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            final long startIndex =  i * slices;
            System.out.println("Spawning thread with start=" + startIndex + " and length=" + slices);

            threads.add(CompletableFuture.supplyAsync(() -> readFile(startIndex, slices), executorService));
        }

        for (CompletableFuture<Long> thread : threads) {
            occurrences += thread.get();
        }

        Instant finish = Instant.now();
        System.out.println("Occurrences found=[" + occurrences + "] within " + Duration.between(startInterval, finish).toMillis() + " ms");

        executorService.shutdown();
    }


    private static long readFile(final long start, final long length) {
        long counter = 0;

        try(final RandomAccessFile file = new RandomAccessFile(PATH, "r")) {
            final MappedByteBuffer mb = file.getChannel().map(FileChannel.MapMode.READ_ONLY, start, length);

            byte[] bufferArray = new byte[BUFFER_SIZE];
            int bufferSize;

            while(mb.hasRemaining()) {
                bufferSize = Math.min(mb.remaining(), BUFFER_SIZE);

                mb.get(bufferArray, 0, bufferSize);

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
        }  catch (IOException e) {
            e.printStackTrace();
        }

        return counter;
    }
}
