/**
 * @file   tiledb_array_create_dense.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
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
 * It shows how to create a dense array.
 *
 * Program output:
 *        $ ./tiledb_array_create_dense
 *        - Array name: <current_working_dir>/my_dense_array
 *        - Array type: dense
 *        - Cell order: row-major
 *        - Tile order: row-major
 *        - Capacity: 10000
 *
 *        ### Dimension ###
 *        - Name: d1
 *        - Type: INT64
 *        - Compressor: NO_COMPRESSION
 *        - Compression level: -1
 *        - Domain: [1,4]
 *        - Tile extent: 2
 *
 *        ### Dimension ###
 *        - Name: d2
 *        - Type: INT64
 *        - Compressor: NO_COMPRESSION
 *        - Compression level: -1
 *        - Domain: [1,4]
 *        - Tile extent: 2
 *
 *        ### Attribute ###
 *        - Name: a1
 *        - Type: INT32
 *        - Compressor: RLE
 *        - Compression level: -1
 *        - Cell val num: 1
 *
 *        ### Attribute ###
 *        - Name: a2
 *        - Type: CHAR
 *        - Compressor: GZIP
 *        - Compression level: -1
 *        - Cell val num: var
 *
 *        ### Attribute ###
 *        - Name: a3
 *        - Type: FLOAT32
 *        - Compressor: ZSTD
 *        - Compression level: -1
 *        - Cell val num: 2
 */

#include "tiledb.h"

#include <cstdlib>

int main() {
  // Initialize context with the default configuration parameters
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);

  // Delete existing array
  system("rm -rf my_dense_array");

  // Array name
  const char* array_name = "my_dense_array";

  // Attributes
  tiledb_attribute_t* a1;
  tiledb_attribute_create(ctx, &a1, "a1", TILEDB_INT32);
  tiledb_attribute_set_compressor(ctx, a1, TILEDB_DOUBLE_DELTA, -1);
  tiledb_attribute_set_cell_val_num(ctx, a1, 1);
  tiledb_attribute_t* a2;
  tiledb_attribute_create(ctx, &a2, "a2", TILEDB_CHAR);
  tiledb_attribute_set_compressor(ctx, a2, TILEDB_DOUBLE_DELTA, -1);
  tiledb_attribute_set_cell_val_num(ctx, a2, tiledb_var_num());
  tiledb_attribute_t* a3;
  tiledb_attribute_create(ctx, &a3, "a3", TILEDB_FLOAT32);
  tiledb_attribute_set_compressor(ctx, a3, TILEDB_ZSTD, -1);
  tiledb_attribute_set_cell_val_num(ctx, a3, 2);

  // Domain and tile extents
  int64_t dim_domain[] = {1, 4, 1, 4};
  int64_t tile_extents[] = {2, 2};

  // Hyperspace
  tiledb_domain_t* domain;
  tiledb_domain_create(ctx, &domain, TILEDB_INT64);
  tiledb_domain_add_dimension(
      ctx, domain, "d1", &dim_domain[0], &tile_extents[0]);
  tiledb_domain_add_dimension(
      ctx, domain, "d2", &dim_domain[2], &tile_extents[1]);

  // Create array_metadata metadata
  tiledb_array_metadata_t* array_metadata;
  tiledb_array_metadata_create(ctx, &array_metadata, array_name);
  tiledb_array_metadata_set_array_type(ctx, array_metadata, TILEDB_DENSE);
  tiledb_array_metadata_set_domain(ctx, array_metadata, domain);
  tiledb_array_metadata_add_attribute(ctx, array_metadata, a1);
  tiledb_array_metadata_add_attribute(ctx, array_metadata, a2);
  tiledb_array_metadata_add_attribute(ctx, array_metadata, a3);

  // Check array metadata
  if (tiledb_array_metadata_check(ctx, array_metadata) != TILEDB_OK) {
    printf("Invalid array metadata\n");
    return -1;
  }

  // Create array
  tiledb_array_create(ctx, array_metadata);

  // Load and dump array metadata to make sure the array was created correctly
  tiledb_array_metadata_t* loaded_array_metadata;
  tiledb_array_metadata_load(ctx, &loaded_array_metadata, array_name);
  tiledb_array_metadata_dump(ctx, loaded_array_metadata, stdout);

  // Clean up
  tiledb_attribute_free(ctx, a1);
  tiledb_attribute_free(ctx, a2);
  tiledb_attribute_free(ctx, a3);
  tiledb_domain_free(ctx, domain);
  tiledb_array_metadata_free(ctx, array_metadata);
  tiledb_ctx_free(ctx);

  return 0;
}
