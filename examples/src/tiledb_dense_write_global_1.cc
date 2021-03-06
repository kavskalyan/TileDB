/**
 * @file   tiledb_dense_write_global_1.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * It shows how to write an entire dense array in a single write.
 *
 * You need to run the following to make this work:
 *
 * ./tiledb_dense_create
 * ./tiledb_dense_write_global_1
 */

#include <tiledb.h>

int main() {
  // Initialize context with the default configuration parameters
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Prepare cell buffers
  // clang-format on
  int buffer_a1[] = {
      0,
      1,
      2,
      3,  // Upper left tile
      4,
      5,
      6,
      7,  // Upper right tile
      8,
      9,
      10,
      11,  // Lower left tile
      12,
      13,
      14,
      15  // Lower right tile
  };
  uint64_t buffer_a2[] = {
      0,
      1,
      3,
      6,  // Upper left tile
      10,
      11,
      13,
      16,  // Upper right tile
      20,
      21,
      23,
      26,  // Lower left tile
      30,
      31,
      33,
      36  // Lower right tile
  };
  char buffer_var_a2[] =
      "abbcccdddd"   // Upper left tile
      "effggghhhh"   // Upper right tile
      "ijjkkkllll"   // Lower left tile
      "mnnooopppp";  // Lower right tile
  float buffer_a3[] = {
      0.1,  0.2,  1.1,  1.2,  2.1,  2.2,  3.1,  3.2,   // Upper left tile
      4.1,  4.2,  5.1,  5.2,  6.1,  6.2,  7.1,  7.2,   // Upper right tile
      8.1,  8.2,  9.1,  9.2,  10.1, 10.2, 11.1, 11.2,  // Lower left tile
      12.1, 12.2, 13.1, 13.2, 14.1, 14.2, 15.1, 15.2,  // Lower right tile
  };
  void* buffers[] = {buffer_a1, buffer_a2, buffer_var_a2, buffer_a3};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3)};
  // clang-format off

  // Create query
  tiledb_query_t* query;
  tiledb_query_create(
    ctx,
    &query,
    "my_dense_array",
    TILEDB_WRITE,
    TILEDB_GLOBAL_ORDER,
    nullptr,
    nullptr,
    0,
    buffers,
    buffer_sizes);

  // Submit query
  tiledb_query_submit(ctx, query);

  // Clean up
  tiledb_query_free(ctx, query);
  tiledb_ctx_free(ctx);

  return 0;
}
