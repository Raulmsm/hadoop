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

import org.apache.hadoop.conf.Configuration;

/**
 * Run the base Azure file system tests strictly on append blobs to make sure
 * fundamental operations on append blob files and folders work as expected.
 * These operations include create, delete, rename, list, and so on.
 */
public class TestNativeAzureFSAppendBlobLive
    extends NativeAzureFileSystemBaseTest {

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    Configuration conf = new Configuration();

    // Configure the page blob directories key so every file created is an
    // append blob.
    conf.set(AzureNativeFileSystemStore.KEY_APPEND_BLOB_DIRECTORIES, "/");

    return AzureBlobStorageTestAccount.create(conf);
  }
}
