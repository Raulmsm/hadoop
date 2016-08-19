/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCrudAppendBlobCompatibility {
  private final int ONE_MEGABYTE = 1024 * 1024;

  private FileSystem fs;
  private AzureBlobStorageTestAccount testAccount;
  private Random rand = new Random();

  protected static String appendBlobDirectory = AzureBlobStorageTestAccount.DEFAULT_APPEND_BLOB_DIRECTORY;

  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }

  @Before
  public void setUp() throws Exception {
    testAccount = createTestAccount();
    if (testAccount != null) {
      fs = testAccount.getFileSystem();
      isAppendBlob(fs, appendBlobDirectory + "/");
    }

    assumeNotNull(testAccount);
  }

  @After
  public void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
      fs = null;
    }
  }

  @Test
  public void TestCreateAppendBlobOverwrites() throws Exception {
    String blobPath = appendBlobDirectory + "/foo";
    Path blobToCreate = new Path("/" + blobPath);
    FSDataOutputStream createdBlobOutStream = fs.create(blobToCreate);
    assertTrue(fs.exists(blobToCreate));
    assertTrue(isAppendBlob(fs, blobPath + "/"));

    // Write 1 byte to the blob and verify that the size is accounted for
    createdBlobOutStream.write(new byte[] { 1 });
    createdBlobOutStream.hflush();
    FileStatus blobStatus = fs.getFileStatus(blobToCreate);
    assertEquals(1, blobStatus.getLen());

    // Creating a new blob with the same name overrides the old one
    fs.create(blobToCreate);
    blobStatus = fs.getFileStatus(blobToCreate);
    assertEquals(0, blobStatus.getLen());

    createdBlobOutStream.close();
  }

  @Test
  public void TestAppendBlobReadWrite() throws Exception {
    int fiveMegaBytes = 5 * ONE_MEGABYTE;
    byte[] randomBytes = generateRandomBytes(fiveMegaBytes);
    String blobPath = appendBlobDirectory + "/foo";
    Path appendBlob = new Path("/" + blobPath);
    FSDataOutputStream createdBlobOutStream = fs.create(appendBlob);
    assertTrue(fs.exists(appendBlob));
    assertTrue(isAppendBlob(fs, blobPath + "/"));

    // Write 5 MB (above block size)
    createdBlobOutStream.write(randomBytes);
    createdBlobOutStream.hflush();
    FileStatus blobStatus = fs.getFileStatus(appendBlob);
    assertEquals(fiveMegaBytes, blobStatus.getLen());

    // Read without closing the writer stream intentionally
    FSDataInputStream blobInStream = fs.open(appendBlob);
    byte[] readBytes = new byte[fiveMegaBytes];
    blobInStream.read(readBytes, 0, fiveMegaBytes);
    assertTrue(Arrays.equals(randomBytes, readBytes));

    createdBlobOutStream.close();
    blobInStream.close();

    // Check that the file stays as-is after closing
    blobStatus = fs.getFileStatus(appendBlob);
    assertEquals(fiveMegaBytes, blobStatus.getLen());

    FSDataInputStream reopenBlobStream = fs.open(appendBlob);
    reopenBlobStream.read(readBytes, 0, fiveMegaBytes);
    assertTrue(Arrays.equals(randomBytes, readBytes));
    reopenBlobStream.close();
  }

  @Test
  public void TestAppendBlobSeekRead() throws Exception {
    int sevenMegaBytes = 7 * ONE_MEGABYTE;
    int seekPosition = 4 * ONE_MEGABYTE - 1;
    String blobPath = appendBlobDirectory + "/foo";
    byte[] randomBytes = generateRandomBytes(sevenMegaBytes);

    Path appendBlob = new Path("/" + blobPath);
    FSDataOutputStream createdBlobOutStream = fs.create(appendBlob);
    assertTrue(fs.exists(appendBlob));
    assertTrue(isAppendBlob(fs, blobPath + "/"));

    createdBlobOutStream.write(randomBytes);
    createdBlobOutStream.hflush();
    createdBlobOutStream.close();
    FileStatus blobStatus = fs.getFileStatus(appendBlob);
    assertEquals(sevenMegaBytes, blobStatus.getLen());

    byte[] targetChunk = new byte[ONE_MEGABYTE];
    byte[] readAfterSeekBytes = new byte[ONE_MEGABYTE];
    System.arraycopy(randomBytes, seekPosition, targetChunk, 0, ONE_MEGABYTE);
    FSDataInputStream blobInStream = fs.open(appendBlob);
    blobInStream.seek(seekPosition);
    blobInStream.read(readAfterSeekBytes, 0, ONE_MEGABYTE);

    assertTrue(Arrays.equals(targetChunk, readAfterSeekBytes));
    blobInStream.close();
  }

  /**
   * This test relies on a pre-defined directory being configured to default to
   * Append blobs.
   */
  private boolean isAppendBlob(FileSystem fs, String key) {
    AzureNativeFileSystemStore store = ((NativeAzureFileSystem) fs).getStore();
    return store.isAppendBlobKey(key);
  }

  private byte[] generateRandomBytes(int numBytes) {
    byte[] randomData = new byte[numBytes];
    rand.nextBytes(randomData);
    return randomData;
  }
}
