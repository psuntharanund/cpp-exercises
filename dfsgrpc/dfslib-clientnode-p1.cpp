#include <regex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>

#include "dfslib-shared-p1.h"
#include "dfslib-clientnode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;
using dfs_service::DFSService;
using dfs_service::FileRequest;
using dfs_service::ListRequest;
using dfs_service::ListReply;
using dfs_service::FileChunk;
using dfs_service::FileMetaData;
using dfs_service::FileStatus;
using dfs_service::StoreRequest;
using dfs_service::StoreReply;
using dfs_service::DeleteReply;

//
// STUDENT INSTRUCTION:
//
// You may want to add aliases to your namespaced service methods here.
// All of the methods will be under the `dfs_service` namespace.
//
// For example, if you have a method named MyMethod, add
// the following:
//
//      using dfs_service::MyMethod
//


DFSClientNodeP1::DFSClientNodeP1() : DFSClientNode() {
}

DFSClientNodeP1::~DFSClientNodeP1() noexcept {}

StatusCode DFSClientNodeP1::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept and store a file.
    //
    // When working with files in gRPC you'll need to stream
    // the file contents, so consider the use of gRPC's ClientWriter.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the client
    // StatusCode::CANCELLED otherwise
    //

    std::string fullPath = this->WrapPath(filename);

    struct stat st;
    if (stat(fullPath.c_str(), &st) != 0){
        return StatusCode::NOT_FOUND;
    }

    std::ifstream ifs(fullPath, std::ios::binary);
    if (!ifs.is_open()){
        return StatusCode::NOT_FOUND;
    }

    ClientContext ctx;
    ctx.set_deadline(
            std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout)
            );
    

    StoreReply reply;
    std::unique_ptr<ClientWriter<StoreRequest>> writer(this->service_stub->Store(&ctx, &reply));

    StoreRequest meta_request;
    FileMetaData* meta = meta_request.mutable_meta();
    meta->set_filename(filename);
    meta->set_size(static_cast<uint64_t>(st.st_size));
    meta->set_mtime(static_cast<int64_t>(st.st_mtime));

    if (!writer->Write(meta_request)){
        ifs.close();
        writer->WritesDone();
        Status status = writer->Finish();
        return status.error_code();
    }

    char buf[2048];
    while (ifs.good()){
        ifs.read(buf, sizeof(buf));
        std::streamsize sent = ifs.gcount();
        if (sent > 0){
            StoreRequest chunk_request;
            chunk_request.set_data(buf, static_cast<size_t>(sent));
            if (!writer->Write(chunk_request)){
                ifs.close();
                writer->WritesDone();
                Status status = writer->Finish();
                return status.error_code();
            }
        }
    }

    ifs.close();
    writer->WritesDone();
    Status status = writer->Finish();

    if (status.ok()){
        return StatusCode::OK;
    }

    if (status.error_code() == StatusCode::DEADLINE_EXCEEDED){
        return StatusCode::DEADLINE_EXCEEDED;
    }

    return StatusCode::CANCELLED;
}


StatusCode DFSClientNodeP1::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept a file request and return the contents
    // of a file from the service.
    //
    // As with the store function, you'll need to stream the
    // contents, so consider the use of gRPC's ClientReader.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    
    ClientContext ctx;
    ctx.set_deadline(
            std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout)
            );
    

    FileRequest request;
    request.set_filename(filename);

    std::string fullPath = this->WrapPath(filename);
    std::ofstream ofs(fullPath, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()){
        return StatusCode::CANCELLED;
    }

    std::unique_ptr<ClientReader<FileChunk>> reader(this->service_stub->Fetch(&ctx, request));

    FileChunk chunk;
    while (reader->Read(&chunk)){
        ofs.write(chunk.data().data(), chunk.data().size());
        if (!ofs.good()){
            ofs.close();
            Status status = reader->Finish();
            return StatusCode::CANCELLED;
        }
    }

    ofs.close();
    Status status = reader->Finish();

    if (status.ok()){
        return StatusCode::OK;
    }

    if (status.error_code() == StatusCode::NOT_FOUND){
        return StatusCode::NOT_FOUND;
    }

    if (status.error_code() == StatusCode::DEADLINE_EXCEEDED){
        return StatusCode::DEADLINE_EXCEEDED;
    }

    return StatusCode::CANCELLED;
}

StatusCode DFSClientNodeP1::Delete(const std::string& filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    
    ClientContext ctx;
    ctx.set_deadline(
            std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout)
            );
    

    FileRequest request;
    request.set_filename(filename);

    DeleteReply reply;
    Status status = this->service_stub->Delete(&ctx, request, &reply);

    if (status.ok()){
        return StatusCode::OK;
    }

    if (status.error_code() == StatusCode::NOT_FOUND){
        return StatusCode::NOT_FOUND;
    }

    if (status.error_code() == StatusCode::DEADLINE_EXCEEDED){
        return StatusCode::DEADLINE_EXCEEDED;
    }

    return StatusCode::CANCELLED;
}

StatusCode DFSClientNodeP1::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list all files here. This method
    // should connect to your service's list method and return
    // a list of files using the message type you created.
    //
    // The file_map parameter is a simple map of files. You should fill
    // the file_map with the list of files you receive with keys as the
    // file name and values as the modified time (mtime) of the file
    // received from the server.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //

    ClientContext ctx;
    ctx.set_deadline(
            std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout)
            );
    

    ListRequest request;
    ListReply reply;

    Status status = this->service_stub->List(&ctx, request, &reply);

    if (!status.ok()){
        if (status.error_code() == StatusCode::DEADLINE_EXCEEDED){
            return StatusCode::DEADLINE_EXCEEDED;
        }
        return StatusCode::CANCELLED;
    }

    file_map->clear();
    for (const auto& file : reply.files()){
        (*file_map)[file.filename()] = static_cast<int>(file.mtime());
        if (display){
            std::cout << file.filename() << " " << file.mtime() << std::endl;
        }
    }

    return StatusCode::OK;
}

StatusCode DFSClientNodeP1::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. This method should
    // retrieve the status of a file on the server. Note that you won't be
    // tested on this method, but you will most likely find that you need
    // a way to get the status of a file in order to synchronize later.
    //
    // The status might include data such as name, size, mtime, crc, etc.
    //
    // The file_status is left as a void* so that you can use it to pass
    // a message type that you defined. For instance, may want to use that message
    // type after calling Stat from another method.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //
    
    ClientContext ctx;
    ctx.set_deadline(
            std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout)
            );
    

    FileRequest request;
    request.set_filename(filename);

    FileStatus response;
    Status status = this->service_stub->Stat(&ctx, request, &response);

    if (!status.ok()){
        if (status.error_code() == StatusCode::NOT_FOUND){
            return StatusCode::NOT_FOUND;
        }
        if (status.error_code() == StatusCode::DEADLINE_EXCEEDED){
            return StatusCode::DEADLINE_EXCEEDED;
        }

        return StatusCode::CANCELLED;
    }

    if (file_status != nullptr){
        *static_cast<FileStatus*>(file_status) = response;
    }

    return StatusCode::OK;
}

//
// STUDENT INSTRUCTION:
//
// Add your additional code here, including
// implementations of your client methods
//


