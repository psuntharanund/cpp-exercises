#include <regex>
#include <mutex>
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
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
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

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using to indicate
// a file request and a listing of files from the server.
//
using FileRequestType = FileRequest;
using FileListResponseType = ListReply;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}

grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to obtain a write lock here when trying to store a file.
    // This method should request a write lock for the given file at the server,
    // so that the current client becomes the sole creator/writer. If the server
    // responds with a RESOURCE_EXHAUSTED response, the client should cancel
    // the current file storage
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
    
    ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(this->deadline_timeout));

    FileRequest request;
    request.set_filename(filename);
    request.set_client_id(this->ClientID);

    LockReply reply;

    Status status = this->service_stub->RequestWriteLock(&ctx, request, &reply);

    if (status.ok()){
        return StatusCode::OK;
    }

    if (status.error_code() == StatusCode::RESOURCE_EXHAUSTED){
        return StatusCode::RESOURCE_EXHAUSTED;
    }

    if (status.error_code() == StatusCode::DEADLINE_EXCEEDED){
        return StatusCode::DEADLINE_EXCEEDED;
    }

    return StatusCode::CANCELLED;
}

grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // stored is the same on the server (i.e. the ALREADY_EXISTS gRPC response).
    //
    // You will also need to add a request for a write lock before attempting to store.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::ALREADY_EXISTS - if the local cached file has not changed from the server version
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
   
    auto lock = RequestWriteAccess(filename);
    if (lock != StatusCode::OK){
        return lock;
    }

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


grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // fetched is the same on the client (i.e. the files do not differ
    // between the client and server and a fetch would be unnecessary.
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // DEADLINE_EXCEEDED - if the deadline timeout occurs
    // NOT_FOUND - if the file cannot be found on the server
    // ALREADY_EXISTS - if the local cached file has not changed from the server version
    // CANCELLED otherwise
    //
    // Hint: You may want to match the mtime on local files to the server's mtime
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

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You will also need to add a request for a write lock before attempting to delete.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
   
    auto lock = RequestWriteAccess(filename);
    if (lock != StatusCode::OK){
        return lock;
    }

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

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list files here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // listing details that would be useful to your solution to the list response.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
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

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // status details that would be useful to your solution.
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

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback) {

    //
    // STUDENT INSTRUCTION:
    //
    // This method gets called each time inotify signals a change
    // to a file on the file system. That is every time a file is
    // modified or created.
    //
    // You may want to consider how this section will affect
    // concurrent actions between the inotify watcher and the
    // asynchronous callbacks associated with the server.
    //
    // The callback method shown must be called here, but you may surround it with
    // whatever structures you feel are necessary to ensure proper coordination
    // between the async and watcher threads.
    //
    // Hint: how can you prevent race conditions between this thread and
    // the async thread when a file event has been signaled?
    //

    std::lock_guard<std::mutex> guard(this->sync_m);
    callback();

}

//
// STUDENT INSTRUCTION:
//
// This method handles the gRPC asynchronous callbacks from the server.
// We've provided the base structure for you, but you should review
// the hints provided in the STUDENT INSTRUCTION sections below
// in order to complete this method.
//
void DFSClientNodeP2::HandleCallbackList() {

    void* tag;

    bool ok = false;

    //
    // STUDENT INSTRUCTION:
    //
    // Add your file list synchronization code here.
    //
    // When the server responds to an asynchronous request for the CallbackList,
    // this method is called. You should then synchronize the
    // files between the server and the client based on the goals
    // described in the readme.
    //
    // In addition to synchronizing the files, you'll also need to ensure
    // that the async thread and the file watcher thread are cooperating. These
    // two threads could easily get into a race condition where both are trying
    // to write or fetch over top of each other. So, you'll need to determine
    // what type of locking/guarding is necessary to ensure the threads are
    // properly coordinated.
    //

    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok)) {
        {
            //
            // STUDENT INSTRUCTION:
            //
            // Consider adding a critical section or RAII style lock here
            //
            std::lock_guard<std::mutex> guard(this->sync_m);

            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            // GPR_ASSERT(ok);
            if (!ok) {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok()) {

                dfs_log(LL_DEBUG3) << "Handling async callback ";

                //
                // STUDENT INSTRUCTION:
                //
                // Add your handling of the asynchronous event calls here.
                // For example, based on the file listing returned from the server,
                // how should the client respond to this updated information?
                // Should it retrieve an updated version of the file?
                // Send an update to the server?
                // Do nothing?
                //
                
                const FileListResponseType& server_reply = call_data->reply;

                //build server map
                std::map<std::string, FileMetaData> server_map;
                for (const auto& file : server_reply.files()){
                    server_map[file.filename()] = file;
                }

                //build local map
                auto local_map = BuildLocalMap();
                
                //reconcile files present on server
                for (const auto& [filename, server_meta] : server_map){
                    auto local_it = local_map.find(filename);

                    //if the file is missing locally, get it from the server
                    if (local_it == local_map.end()){
                        dfs_log(LL_DEBUG3) << "Missing file locally, fetching from server." << filename;
                        this->Fetch(filename);
                        continue;
                    }

                    const LocalFileInfo& local_info = local_it->seconds;

                    //if file already exists locally, do nothing
                    if (local_info.crc == server_meta.crc()){
                        continue;
                    }

                    //if the contents of files differ from local to server, pick newer mtime file
                    if (server_meta.mtime() > local_info.mtime){
                        dfs_log(LL_DEBUG3) << "Server has newer file, fetching. . ." << filename;
                        this->Fetch(filename);
                    } else if (server_meta.mtime() < local_info.mtime){
                        dfs_log(LL_DEBUG3) << "Client has newer file, storing. . ." << filename;
                        this->Store(filename);
                    } else{
                        //mtime equal, but CRC differs do nothing
                        dfs_log(LL_DEBUG3) << "Equal mtimes, but differing CRC. Doing nothing. . ." << filename;
                    }
                }

                //reconcile files present locally but not on server
                for (const auto& [filename, server_meta] : local_map){
                    if (server_map.find(filename == server_map.end()){
                        dfs_log(LL_DEBUG3) << "File does not exist on server, storing. . ." << filename;
                        this->Store(filename);
                    }
                }

            } else {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;

            //
            // STUDENT INSTRUCTION:
            //
            // Add any additional syncing/locking mechanisms you may need here

        }


        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();

    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 *
 * We're making use of a template function here, so that we can keep some
 * of the more intricate workings of the async process out of the way, and
 * give you a chance to focus more on the project's requirements.
 */
void DFSClientNodeP2::InitCallbackList() {
    CallbackList<FileRequestType, FileListResponseType>();
}

//
// STUDENT INSTRUCTION:
//
// Add any additional code you need to here
//

std::map<std::string, DFSClientNodeP2::LocalFileInfo> DFSClientNodeP2::BuildLocalMap(){
    std::map<std::string, LocalFileInfo> local_map;

    DIR* directory = opendir(this->WrapPath().c_str());
    if (!directory){
        return local_map;
    }

    struct dirent* entry;
    while ((entry = readdir(directory)) != nullptr){
        std::string name = entry->d_name;
        if (name == "." || name == ".."){
            continue;
        }

        std::string fullPath = this->WrapPath(name);
        struct stat st;
        if (stat(fullPath.c_str(), &st) == 0 && S_ISREG(st.st_mode)){
            LocalFileInfo info;
            info.filename = name;
            info.size = static_cast<std::uint64_t>(st.st_size);
            info.mtime = static_cast<std::int64_t>(st.st_mtime);
            info.crc = dfs_file_checksum(fullPath, &this->crc_table);
            local_map[name] = info;
        }
    }

    closedir(directory);
    return local_map;
}
