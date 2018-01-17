#include <tiledb.h>
#include <chrono>
#include <iostream>

const std::string URI_PREFIX = "hdfs://";
const std::string DIRNAME = "/tmp";
const std::string FILENAME = "/dense_array_1";
void initialize_buffers(int* buffer1, int block_size){
  for (int i = 0; i < block_size; i++){
      buffer1[i] = rand() % 100;
  }
}

int main(int argc, char** argv) {
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);
  std::string array_name_string = URI_PREFIX + DIRNAME + FILENAME;
  int number_of_iterations = 32;
  int block_size = 128;
  if(argc > 1){
    int file_size = atoi(argv[1]);
    block_size = atoi(argv[2]);
    number_of_iterations = file_size/block_size;
    array_name_string = URI_PREFIX + argv[3];
  }

  // Prepare cell buffers
  int buffer_count = (block_size * 1024 * 1024)/4;
  int * buffer_a1 = new int[buffer_count];
  uint64_t buffer_size = buffer_count * sizeof(int);
  std::cout<<buffer_count <<std::endl;
  initialize_buffers(buffer_a1, buffer_count);
  void* buffers[] = {buffer_a1};
  uint64_t buffer_sizes[] = {
  	buffer_size
  };

  const char* array_name = array_name_string.c_str();
  using std::chrono::steady_clock;
  auto start = steady_clock::now();
  // Create query
  tiledb_query_t* query;
  tiledb_query_create(
    ctx,
    &query,
    array_name,
    TILEDB_WRITE,
    TILEDB_GLOBAL_ORDER,
    nullptr,
    nullptr,
    0,
    buffers,
    buffer_sizes);

  // Submit query
  tiledb_query_submit(ctx, query);

  for(int i = 1; i < number_of_iterations; i++){
    tiledb_query_reset_buffers(ctx, query, buffers, buffer_sizes);
    tiledb_query_submit(ctx, query);
  }
  auto end = steady_clock::now();
  double elapsedSeconds = ((end - start).count()) * steady_clock::period::num / static_cast<double>(steady_clock::period::den);
  std::cout << "Time taken to write (in seconds):" << elapsedSeconds << std::endl;
  // Clean up
  tiledb_query_free(ctx, query);
  tiledb_ctx_free(ctx);
  delete [] buffer_a1;
  return 0;
}
