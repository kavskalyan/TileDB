/**
 * @file   gzip_compressor.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file defines the gzip compressor class.
 */

#ifndef TILEDB_GZIP_H
#define TILEDB_GZIP_H

#include "buffer.h"
#include "status.h"

#include <cmath>

namespace tiledb {

/** Handles compression/decompression with the zlib (gzip) library. */
class GZip {
 public:
  /**
   * Compression function.
   *
   * @param level Compression level.
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write to the compressed data.
   * @return Status
   */
  static Status compress(
      int level, const Buffer* input_buffer, Buffer* output_buffer);

  /**
   * Decompression function.
   *
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write the decompressed data to.
   * @return Status
   */
  static Status decompress(const Buffer* input_buffer, Buffer* output_buffer);

  /**
   * Extra overhead of GZip given a buffer size.
   *
   * @param buffer_size The size to calculate the overhead on.
   * @return The computed overhead.
   */
  static uint64_t overhead(uint64_t buffer_size) {
    return 6 + 5 * uint64_t((ceil(buffer_size / 16834.0)));
  }

  /** Returns the default compression level. */
  static int default_level() {
    return -1;
  }
};

}  // namespace tiledb

#endif  // TILEDB_GZIP_H
