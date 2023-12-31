/*
 * Copyright (C) 2016 Square, Inc.
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
package okio;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public final class PipeTest {
  final ScheduledExecutorService executorService = TestingExecutors.INSTANCE.newScheduledExecutorService(2);

  @After
  public void tearDown() throws Exception {
    executorService.shutdown();
  }

  @Test
  public void test() throws Exception {
    Pipe pipe = new Pipe(6);
    pipe.sink().write(new Buffer().writeUtf8("abc"), 3L);

    Source source = pipe.source();
    Buffer readBuffer = new Buffer();
    assertEquals(3L, source.read(readBuffer, 6L));
    assertEquals("abc", readBuffer.readUtf8());

    pipe.sink().close();
    assertEquals(-1L, source.read(readBuffer, 6L));

    source.close();
  }

  /**
   * A producer writes the first 16 MiB of bytes generated by {@code new Random(0)} to a sink, and a
   * consumer consumes them. Both compute hashes of their data to confirm that they're as expected.
   */
  @Test
  public void largeDataset() throws Exception {
    final Pipe pipe = new Pipe(1000L); // An awkward size to force producer/consumer exchange.
    final long totalBytes = 16L * 1024L * 1024L;
    ByteString expectedHash = ByteString.decodeHex("7c3b224bea749086babe079360cf29f98d88262d");

    // Write data to the sink.
    Future<ByteString> sinkHash = executorService.submit(new Callable<ByteString>() {
      @Override
      public ByteString call() throws Exception {
        HashingSink hashingSink = HashingSink.sha1(pipe.sink());
        Random random = new Random(0);
        byte[] data = new byte[8192];

        Buffer buffer = new Buffer();
        for (long i = 0L; i < totalBytes; i += data.length) {
          random.nextBytes(data);
          buffer.write(data);
          hashingSink.write(buffer, buffer.size());
        }

        hashingSink.close();
        return hashingSink.hash();
      }
    });

    // Read data from the source.
    Future<ByteString> sourceHash = executorService.submit(new Callable<ByteString>() {
      @Override
      public ByteString call() throws Exception {
        Buffer blackhole = new Buffer();
        HashingSink hashingSink = HashingSink.sha1(blackhole);

        Buffer buffer = new Buffer();
        while (pipe.source().read(buffer, Long.MAX_VALUE) != -1) {
          hashingSink.write(buffer, buffer.size());
          blackhole.clear();
        }

        pipe.source().close();
        return hashingSink.hash();
      }
    });

    assertEquals(expectedHash, sinkHash.get());
    assertEquals(expectedHash, sourceHash.get());
  }

  @Test
  public void sinkTimeout() throws Exception {
    TestUtil.INSTANCE.assumeNotWindows();

    Pipe pipe = new Pipe(3);
    pipe.sink().timeout().timeout(1000, TimeUnit.MILLISECONDS);
    pipe.sink().write(new Buffer().writeUtf8("abc"), 3L);
    double start = now();
    try {
      pipe.sink().write(new Buffer().writeUtf8("def"), 3L);
      fail();
    } catch (InterruptedIOException expected) {
      assertEquals("timeout", expected.getMessage());
    }
    assertElapsed(1000.0, start);

    Buffer readBuffer = new Buffer();
    assertEquals(3L, pipe.source().read(readBuffer, 6L));
    assertEquals("abc", readBuffer.readUtf8());
  }

  @Test
  public void sourceTimeout() throws Exception {
    TestUtil.INSTANCE.assumeNotWindows();

    Pipe pipe = new Pipe(3L);
    pipe.source().timeout().timeout(1000, TimeUnit.MILLISECONDS);
    double start = now();
    Buffer readBuffer = new Buffer();
    try {
      pipe.source().read(readBuffer, 6L);
      fail();
    } catch (InterruptedIOException expected) {
      assertEquals("timeout", expected.getMessage());
    }
    assertElapsed(1000.0, start);
    assertEquals(0, readBuffer.size());
  }

  /**
   * The writer is writing 12 bytes as fast as it can to a 3 byte buffer. The reader alternates
   * sleeping 1000 ms, then reading 3 bytes. That should make for an approximate timeline like
   * this:
   * <p>
   * 0: writer writes 'abc', blocks 0: reader sleeps until 1000
   * 1000: reader reads 'abc', sleeps until 2000
   * 1000: writer writes 'def', blocks
   * 2000: reader reads 'def', sleeps until 3000
   * 2000: writer writes 'ghi', blocks
   * 3000: reader reads 'ghi', sleeps until 4000
   * 3000: writer writes 'jkl', returns
   * 4000: reader reads 'jkl', returns
   * <p>
   * Because the writer is writing to a buffer, it finishes before the reader does.
   */
  @Test
  public void sinkBlocksOnSlowReader() throws Exception {
    final Pipe pipe = new Pipe(3L);
    executorService.execute(new Runnable() {
      @Override
      public void run() {
        try {
          Buffer buffer = new Buffer();
          Thread.sleep(1000L);
          assertEquals(3, pipe.source().read(buffer, Long.MAX_VALUE));
          assertEquals("abc", buffer.readUtf8());
          Thread.sleep(1000L);
          assertEquals(3, pipe.source().read(buffer, Long.MAX_VALUE));
          assertEquals("def", buffer.readUtf8());
          Thread.sleep(1000L);
          assertEquals(3, pipe.source().read(buffer, Long.MAX_VALUE));
          assertEquals("ghi", buffer.readUtf8());
          Thread.sleep(1000L);
          assertEquals(3, pipe.source().read(buffer, Long.MAX_VALUE));
          assertEquals("jkl", buffer.readUtf8());
        } catch (IOException | InterruptedException e) {
          throw new AssertionError();
        }
      }
    });

    double start = now();
    pipe.sink().write(new Buffer().writeUtf8("abcdefghijkl"), 12);
    assertElapsed(3000.0, start);
  }

  @Test
  public void sinkWriteFailsByClosedReader() throws Exception {
    final Pipe pipe = new Pipe(3L);
    executorService.schedule(new Runnable() {
      @Override
      public void run() {
        try {
          pipe.source().close();
        } catch (IOException e) {
          throw new AssertionError();
        }
      }
    }, 1000, TimeUnit.MILLISECONDS);

    double start = now();
    try {
      pipe.sink().write(new Buffer().writeUtf8("abcdef"), 6);
      fail();
    } catch (IOException expected) {
      assertEquals("source is closed", expected.getMessage());
      assertElapsed(1000.0, start);
    }
  }

  @Test
  public void sinkFlushDoesntWaitForReader() throws Exception {
    Pipe pipe = new Pipe(100L);
    pipe.sink().write(new Buffer().writeUtf8("abc"), 3);
    pipe.sink().flush();

    BufferedSource bufferedSource = Okio.buffer(pipe.source());
    assertEquals("abc", bufferedSource.readUtf8(3));
  }

  @Test
  public void sinkFlushFailsIfReaderIsClosedBeforeAllDataIsRead() throws Exception {
    Pipe pipe = new Pipe(100L);
    pipe.sink().write(new Buffer().writeUtf8("abc"), 3);
    pipe.source().close();
    try {
      pipe.sink().flush();
      fail();
    } catch (IOException expected) {
      assertEquals("source is closed", expected.getMessage());
    }
  }

  @Test
  public void sinkCloseFailsIfReaderIsClosedBeforeAllDataIsRead() throws Exception {
    Pipe pipe = new Pipe(100L);
    pipe.sink().write(new Buffer().writeUtf8("abc"), 3);
    pipe.source().close();
    try {
      pipe.sink().close();
      fail();
    } catch (IOException expected) {
      assertEquals("source is closed", expected.getMessage());
    }
  }

  @Test
  public void sinkClose() throws Exception {
    Pipe pipe = new Pipe(100L);
    pipe.sink().close();
    try {
      pipe.sink().write(new Buffer().writeUtf8("abc"), 3);
      fail();
    } catch (IllegalStateException expected) {
      assertEquals("closed", expected.getMessage());
    }
    try {
      pipe.sink().flush();
      fail();
    } catch (IllegalStateException expected) {
      assertEquals("closed", expected.getMessage());
    }
  }

  @Test
  public void sinkMultipleClose() throws Exception {
    Pipe pipe = new Pipe(100L);
    pipe.sink().close();
    pipe.sink().close();
  }

  @Test
  public void sinkCloseDoesntWaitForSourceRead() throws Exception {
    Pipe pipe = new Pipe(100L);
    pipe.sink().write(new Buffer().writeUtf8("abc"), 3);
    pipe.sink().close();

    BufferedSource bufferedSource = Okio.buffer(pipe.source());
    assertEquals("abc", bufferedSource.readUtf8());
    assertTrue(bufferedSource.exhausted());
  }

  @Test
  public void sourceClose() throws Exception {
    Pipe pipe = new Pipe(100L);
    pipe.source().close();
    try {
      pipe.source().read(new Buffer(), 3);
      fail();
    } catch (IllegalStateException expected) {
      assertEquals("closed", expected.getMessage());
    }
  }

  @Test
  public void sourceMultipleClose() throws Exception {
    Pipe pipe = new Pipe(100L);
    pipe.source().close();
    pipe.source().close();
  }

  @Test
  public void sourceReadUnblockedByClosedSink() throws Exception {
    final Pipe pipe = new Pipe(3L);
    executorService.schedule(new Runnable() {
      @Override
      public void run() {
        try {
          pipe.sink().close();
        } catch (IOException e) {
          throw new AssertionError();
        }
      }
    }, 1000, TimeUnit.MILLISECONDS);

    double start = now();
    Buffer readBuffer = new Buffer();
    assertEquals(-1, pipe.source().read(readBuffer, Long.MAX_VALUE));
    assertEquals(0, readBuffer.size());
    assertElapsed(1000.0, start);
  }

  /**
   * The writer has 12 bytes to write. It alternates sleeping 1000 ms, then writing 3 bytes. The
   * reader is reading as fast as it can. That should make for an approximate timeline like this:
   * <p>
   * 0: writer sleeps until 1000
   * 0: reader blocks
   * 1000: writer writes 'abc', sleeps until 2000
   * 1000: reader reads 'abc'
   * 2000: writer writes 'def', sleeps until 3000
   * 2000: reader reads 'def'
   * 3000: writer writes 'ghi', sleeps until 4000
   * 3000: reader reads 'ghi'
   * 4000: writer writes 'jkl', returns
   * 4000: reader reads 'jkl', returns
   */
  @Test
  public void sourceBlocksOnSlowWriter() throws Exception {
    final Pipe pipe = new Pipe(100L);
    executorService.execute(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(1000L);
          pipe.sink().write(new Buffer().writeUtf8("abc"), 3);
          Thread.sleep(1000L);
          pipe.sink().write(new Buffer().writeUtf8("def"), 3);
          Thread.sleep(1000L);
          pipe.sink().write(new Buffer().writeUtf8("ghi"), 3);
          Thread.sleep(1000L);
          pipe.sink().write(new Buffer().writeUtf8("jkl"), 3);
        } catch (IOException | InterruptedException e) {
          throw new AssertionError();
        }
      }
    });

    double start = now();
    Buffer readBuffer = new Buffer();

    assertEquals(3, pipe.source().read(readBuffer, Long.MAX_VALUE));
    assertEquals("abc", readBuffer.readUtf8());
    assertElapsed(1000.0, start);

    assertEquals(3, pipe.source().read(readBuffer, Long.MAX_VALUE));
    assertEquals("def", readBuffer.readUtf8());
    assertElapsed(2000.0, start);

    assertEquals(3, pipe.source().read(readBuffer, Long.MAX_VALUE));
    assertEquals("ghi", readBuffer.readUtf8());
    assertElapsed(3000.0, start);

    assertEquals(3, pipe.source().read(readBuffer, Long.MAX_VALUE));
    assertEquals("jkl", readBuffer.readUtf8());
    assertElapsed(4000.0, start);
  }

  /**
   * Returns the nanotime in milliseconds as a double for measuring timeouts.
   */
  private double now() {
    return System.nanoTime() / 1000000.0d;
  }

  /**
   * Fails the test unless the time from start until now is duration, accepting differences in
   * -50..+450 milliseconds.
   */
  private void assertElapsed(double duration, double start) {
    assertEquals(duration, now() - start - 200d, 250.0);
  }
}
