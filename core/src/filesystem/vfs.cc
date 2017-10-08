/**
 * @file   vfs.cc
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
 * This file implements the VFS class.
 */

#include "vfs.h"
#include "hdfs_filesystem.h"
#include "logger.h"
#include "posix_filesystem.h"

#include <iostream>

int FileIOLogger::abs_path = 0;
int FileIOLogger::create_dir = 0;
int FileIOLogger::create_file = 0;
int FileIOLogger::delete_file = 0;
int FileIOLogger::file_lock = 0;
int FileIOLogger::file_unlock = 0;
int FileIOLogger::file_size = 0;
int FileIOLogger::is_dir = 0;
int FileIOLogger::is_file = 0;
int FileIOLogger::ls = 0;
int FileIOLogger::move_dir = 0;
int FileIOLogger::read_from_file = 0;
int FileIOLogger::write_to_file = 0;
int FileIOLogger::sync = 0;
namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

VFS::VFS() = default;

VFS::~VFS() = default;

/* ********************************* */
/*                API                */
/* ********************************* */

std::string VFS::abs_path(const std::string& path) {
  FileIOLogger::abs_path++;
  FileIOLogger::log_file_io("absolute path of file:" + path);
  if (URI::is_posix(path))
    return posix::abs_path(path);

  // Certainly starts with "<resource>://" other than "file://"
  return path;
}

Status VFS::create_dir(const URI& uri) const {
  FileIOLogger::create_dir++;
  FileIOLogger::log_file_io("Create dir: " + uri.to_path());
  if (uri.is_posix())
    return posix::create_dir(uri.to_path());

  // TODO: Handle all other file systems here !
  return Status::Ok();
}

Status VFS::create_file(const URI& uri) const {
  FileIOLogger::create_file++;
  FileIOLogger::log_file_io("Create file: " + uri.to_path());
  if (uri.is_posix())
    return posix::create_file(uri.to_path());

  // TODO: Handle all other file systems here !
  return Status::Ok();
}

Status VFS::delete_file(const URI& uri) const {
  FileIOLogger::delete_file++;
  FileIOLogger::log_file_io("Delete file: " + uri.to_path());
  if (uri.is_posix())
    return posix::delete_file(uri.to_path());

  // TODO: Handle all other file systems here !
  return Status::Ok();
}

Status VFS::filelock_lock(const URI& uri, int* fd, bool shared) const {
  FileIOLogger::file_lock++;
  FileIOLogger::log_file_io("File lock:" + uri.to_path());
  if (uri.is_posix())
    return posix::filelock_lock(uri.to_path(), fd, shared);

  // TODO: Handle all other file systems here !
  return Status::Ok();
}

Status VFS::filelock_unlock(const URI& uri, int fd) const {
  FileIOLogger::file_unlock++;
  FileIOLogger::log_file_io("file unlock: " + uri.to_path());
  if (uri.is_posix())
    return posix::filelock_unlock(fd);

  // TODO: Handle all other file systems here !
  return Status::Ok();
}

Status VFS::file_size(const URI& uri, uint64_t* size) const {
  FileIOLogger::file_size++;
  FileIOLogger::log_file_io("File size: " + uri.to_path());
  if (uri.is_posix())
    return posix::file_size(uri.to_path(), size);

  // TODO: Handle all other file systems here !
  return Status::Ok();
}

bool VFS::is_dir(const URI& uri) const {
  FileIOLogger::is_dir++;
  FileIOLogger::log_file_io("Is directory: " + uri.to_path());
  if (uri.is_posix())
    return posix::is_dir(uri.to_path());

  // TODO: Handle all other file systems here !
  return true;
}

bool VFS::is_file(const URI& uri) const {
  FileIOLogger::is_file++;
  FileIOLogger::log_file_io("is File: " + uri.to_path());
  if (uri.is_posix())
    return posix::is_file(uri.to_path());

  // TODO: Handle all other file systems here !
  return true;
}

Status VFS::ls(const URI& parent, std::vector<URI>* uris) const {
  FileIOLogger::ls++;
  FileIOLogger::log_file_io("ls on dir:" + parent.to_path());
  if (parent.is_posix()) {
    std::vector<std::string> files;
    RETURN_NOT_OK(posix::ls(parent.to_path(), &files));
    for (auto& file : files)
      uris->push_back(URI(file));
  }

  // TODO: Handle all other file systems here !
  return Status::Ok();
}

Status VFS::move_dir(const URI& old_uri, const URI& new_uri) {
  FileIOLogger::move_dir++;
  FileIOLogger::log_file_io("move from uri: " + old_uri.to_path() + " to new uri: " + new_uri.to_path());
  if (old_uri.is_posix() && new_uri.is_posix())
    return posix::move_dir(old_uri.to_path(), new_uri.to_path());

  // TODO: Handle all other file systems here !
  return Status::Ok();
}

/*
Status VFS::read_from_file(const URI& uri, Buffer** buff) {
  if (uri.is_posix())
    return posix::read_from_file(uri.to_path(), buff);

  // TODO: Handle all other file systems here !
  return Status::Ok();
}
 */

Status VFS::read_from_file(
    const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const {
      FileIOLogger::read_from_file++;
      FileIOLogger::log_file_io("Read file: " + uri.to_path() + " ,Number of bytes:" + std::to_string(nbytes));
  if (uri.is_posix())
    return posix::read_from_file(uri.to_path(), offset, buffer, nbytes);

  // TODO: Handle all other file systems here !
  return Status::Ok();
}

Status VFS::sync(const URI& uri) const {
  FileIOLogger::sync++;
  FileIOLogger::log_file_io("Sync: " + uri.to_path());
  if (uri.is_posix())
    return posix::sync(uri.to_path());

  // TODO: Handle all other file systems here !
  return Status::Ok();
}

Status VFS::write_to_file(
    const URI& uri, const void* buffer, uint64_t buffer_size) const {
      FileIOLogger::write_to_file++;
      FileIOLogger::log_file_io("Write to file: " + uri.to_path() + " ,Number of bytes:" + std::to_string(buffer_size));
  if (uri.is_posix())
    return posix::write_to_file(uri.to_path(), buffer, buffer_size);

  // TODO: Handle all other file systems here !
  return Status::Ok();
}

}  // namespace tiledb
