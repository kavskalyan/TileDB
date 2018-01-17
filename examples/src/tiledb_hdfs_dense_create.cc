#include <tiledb.h>
#include <iostream>
#include <cstdint> 
#include <chrono>
const std::string URI_PREFIX = "hdfs://";
const std::string DIRNAME = "/tmp";
const std::string FILENAME = "/dense_array_1";

int main(int argc, char ** argv) {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);
  using std::chrono::steady_clock;
  auto start = steady_clock::now();
  std::string array_name_string = URI_PREFIX + DIRNAME + FILENAME;
  // Create domain
  if(argc > 1){
    array_name_string = URI_PREFIX + argv[1];
  }
  uint64_t dim_domain[] = {1, 1024 * 32, 1, 32 * 1024};
  uint64_t tile_extents[] = { 256, 256 };
  tiledb_domain_t* domain;
  tiledb_domain_create(ctx, &domain, TILEDB_UINT64);
  tiledb_domain_add_dimension(
      ctx, domain, "d1", &dim_domain[0], &tile_extents[0]);
  tiledb_domain_add_dimension(
      ctx, domain, "d2", &dim_domain[2], &tile_extents[1]);
  // Create attributes
  tiledb_attribute_t* a1;
  tiledb_attribute_create(ctx, &a1, "a1", TILEDB_INT32);
  tiledb_attribute_set_compressor(ctx, a1, TILEDB_BLOSC, -1);
  tiledb_attribute_set_cell_val_num(ctx, a1, 1);
/*
  tiledb_attribute_t* a2;
  tiledb_attribute_create(ctx, &a2, "a2", TILEDB_FLOAT32);
  tiledb_attribute_set_compressor(ctx, a2, TILEDB_ZSTD, -1);
  tiledb_attribute_set_cell_val_num(ctx, a2, 2);
*/
  // Create array metadata
  const char* array_name = array_name_string.c_str();
  tiledb_array_metadata_t* array_metadata;
  tiledb_array_metadata_create(ctx, &array_metadata, array_name);
  tiledb_array_metadata_set_cell_order(ctx, array_metadata, TILEDB_ROW_MAJOR);
  tiledb_array_metadata_set_tile_order(ctx, array_metadata, TILEDB_ROW_MAJOR);
  tiledb_array_metadata_set_array_type(ctx, array_metadata, TILEDB_DENSE);
  tiledb_array_metadata_set_domain(ctx, array_metadata, domain);
  tiledb_array_metadata_add_attribute(ctx, array_metadata, a1);
//  tiledb_array_metadata_add_attribute(ctx, array_metadata, a2);

  // Check array metadata
  if (tiledb_array_metadata_check(ctx, array_metadata) != TILEDB_OK) {
    std::cout << "Invalid array metadata\n";
    return -1;
  }

  // Create array
  tiledb_array_create(ctx, array_metadata);

  // Clean up
  tiledb_attribute_free(ctx, a1);
  //tiledb_attribute_free(ctx, a2);
  tiledb_domain_free(ctx, domain);
  tiledb_array_metadata_free(ctx, array_metadata);
  tiledb_ctx_free(ctx);
  
  auto end = steady_clock::now();
  double elapsedSeconds = ((end - start).count()) * steady_clock::period::num / static_cast<double>(steady_clock::period::den);
  std::cout << "Time taken to create (in seconds):" << elapsedSeconds << std::endl;
  return 0;
}
