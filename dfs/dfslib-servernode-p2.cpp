#include <condition_variable>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>
#include <dirent.h>

#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfs-utils.h"
#include "src/dfslibx-call-data.h"
#include "src/dfslibx-service-runner.h"
#include "dfslib-shared-p2.h"
#include "dfslib-servernode-p2.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

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
using dfs_service::LockRequest;
using dfs_service::LockReply;
using dfs_service::CallbackRequest;
using dfs_service::CallbackReply;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using in your `dfs-service.proto` file
// to indicate a file request and a listing of files from the server
//
using FileRequestType = CallbackRequest;
using FileListResponseType = CallbackReply;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// As with Part 1, the DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in your `dfs-service.proto` file.
//
// You may start with your Part 1 implementations of each service method.
//
// Elements to consider for Part 2:
//
// - How will you implement the write lock at the server level?
// - How will you keep track of which client has a write lock for a file?
//      - Note that we've provided a preset client_id in DFSClientNode that generates
//        a client id for you. You can pass that to the server to identify the current client.
// - How will you release the write lock?
// - How will you handle a store request for a client that doesn't have a write lock?
// - When matching files to determine similarity, you should use the `file_checksum` method we've provided.
//      - Both the client and server have a pre-made `crc_table` variable to speed things up.
//      - Use the `file_checksum` method to compare two files, similar to the following:
//
//          std::uint32_t server_crc = dfs_file_checksum(filepath, &this->crc_table);
//
//      - Hint: as the crc checksum is a simple integer, you can pass it around inside your message types.
//
class DFSServiceImpl final :
    public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
    public DFSCallDataManager<FileRequestType , FileListResponseType> {


private:

    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** The mount path for the server **/
    std::string mount_path;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;

    std::mutex lock_m;
    std::map<std::string,std::string> file_locks;
    std::mutex change_m;
    std::condition_variable change_cv;
    std::uint64_t change_ctr = 0;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }


    void FillServerFileList(ListReply* reply){
        DIR* directory = opendir(this->mount_path.c_str());
        if (!directory){
            return;
        }

        struct dirent* entry;
        while ((entry = readdir(directory)) != nullptr ){
            std::string name = entry->d_name;
            if (name == "." || name == ".."){
                continue;
            }

            std::string fullPath = WrapPath(name);
            struct stat st;

            if (stat(fullPath.c_str(), &st) == 0 && S_ISREG(st.st_mode)){
                FileMetaData* file = reply->add_files();
                file->set_filename(name);
                file->set_size(static_cast<uint64_t>(st.st_size));
                file->set_mtime(static_cast<int64_t>(st.st_mtime));
                file->set_crc(dfs_file_checksum(fullPath, &this->crc_table));
            }
        }

        closedir(directory);
    }

    void FillServerFileList(CallbackReply* reply){
        DIR* directory = opendir(this->mount_path.c_str());
        if (!directory){
            return;
        }

        struct dirent* entry;
        while ((entry = readdir(directory)) != nullptr ){
            std::string name = entry->d_name;
            if (name == "." || name == ".."){
                continue;
            }

            std::string fullPath = WrapPath(name);
            struct stat st;

            if (stat(fullPath.c_str(), &st) == 0 && S_ISREG(st.st_mode)){
                FileStatus* file = reply->add_files();
                file->set_filename(name);
                file->set_size(static_cast<uint64_t>(st.st_size));
                file->set_mtime(static_cast<int64_t>(st.st_mtime));
                file->set_crc(dfs_file_checksum(fullPath, &this->crc_table));
            }
        }

        closedir(directory);
    }


    void CountChange(){
        std::lock_guard<std::mutex> guard(change_m);
        ++change_ctr;
        change_cv.notify_all();
    }
    
    bool HasWriteLock(const std::string& filename, const std::string& clientID){
        std::lock_guard<std::mutex> guard(lock_m);
        auto it = file_locks.find(filename);
        return it != file_locks.end() && it->second == clientID;
    }

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;

public:

    DFSServiceImpl(const std::string& mount_path, const std::string& server_address, int num_async_threads):
        mount_path(mount_path), crc_table(CRC::CRC_32()) {

        this->runner.SetService(this);
        this->runner.SetAddress(server_address);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]{ this->ProcessQueuedRequests(); });

    }

    ~DFSServiceImpl() {
        this->runner.Shutdown();
    }

    void Run() {
        this->runner.Run();
    }
    
    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     * Students should not need to adjust this.
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(grpc::ServerContext* context,
                         FileRequestType* request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType>* response,
                         grpc::ServerCompletionQueue* cq,
                         void* tag) {

        std::lock_guard<std::mutex> lock(queue_mutex);
        this->queued_tags.emplace_back(context, request, response, cq, tag);

    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     * See the STUDENT INSTRUCTION for more details.
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext* context, FileRequestType* request, FileListResponseType* response) {

        //
        // STUDENT INSTRUCTION:
        //
        // You should add your code here to respond to any CallbackList requests from a client.
        // This function is called each time an asynchronous request is made from the client.
        //
        // The client should receive a list of files or modifications that represent the changes this service
        // is aware of. The client will then need to make the appropriate calls based on those changes.
        //
        
        std::uint64_t processing;
        {
            std::lock_guard<std::mutex> guard(change_m);
            processing = change_ctr;
        }

        std::unique_lock<std::mutex> lock(change_m);
        change_cv.wait(lock, [&] {
                return context->IsCancelled() || change_ctr != processing;
                });
        lock.unlock();

        if (context->IsCancelled()){
            return;
        }

        FillServerFileList(response);

    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests() {
        while(true) {

            //
            // STUDENT INSTRUCTION:
            //
            // You should add any synchronization mechanisms you may need here in
            // addition to the queue management. For example, modified files checks.
            //
            // Note: you will need to leave the basic queue structure as-is, but you
            // may add any additional code you feel is necessary.
            //


            // Guarded section for queue
            {
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);


                for(QueueRequest<FileRequestType, FileListResponseType>& queue_request : this->queued_tags) {
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                        queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                    this->queued_tags.begin(),
                    this->queued_tags.end(),
                    [](QueueRequest<FileRequestType, FileListResponseType>& queue_request) { return queue_request.finished; }
                ), this->queued_tags.end());

            }
        }
    }

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // the implementations of your rpc protocol methods.
    //

    Status RequestWriteLock(ServerContext* ctx,
                        const LockRequest* request,
                        LockReply* reply) override{
        std::lock_guard<std::mutex> guard(lock_m);
        std::string file = request->filename();
        std::string client = request->clientid();

        auto it = file_locks.find(file);

        if (it == file_locks.end() || it->second == client){
            file_locks[file] = client;
            reply->set_accepted(true);
            return Status::OK;
        }

        reply->set_accepted(false);
        return Status(StatusCode::RESOURCE_EXHAUSTED, "Lock held by another client.");
    }

    Status Store(ServerContext* ctx, 
                ServerReader<StoreRequest>* reader, 
                StoreReply* reply) override{
        StoreRequest request;
        std::ofstream ofs;
        std::string filename;
        std::string fullPath;
        bool opened = false;

        while (reader->Read(&request)){
            if (request.has_meta()){
                filename = request.meta().filename();
                fullPath = WrapPath(filename);
                
                //detect if file already exists and is unchanged
                struct stat already_exist;
                if (stat(fullPath.c_str(), &already_exist) == 0){
                    std::uint32_t server_crc = dfs_file_checksum(fullPath, &this->crc_table);

                    if (static_cast<uint64_t>(already_exist.st_size) == request.meta().size() &&
                        static_cast<int64_t>(already_exist.st_mtime) == request.meta().mtime() &&
                        server_crc == request.meta().crc()){
                        return Status(StatusCode::ALREADY_EXISTS, "File already exists and is unchanged.");
                    }
                }

                ofs.open(fullPath, std::ios::binary | std::ios::trunc);
                if (!ofs.is_open()){
                    return Status(StatusCode::CANCELLED, "Unable to open file for writing.");
                }
                opened = true;
            } else if(request.payload_case() == StoreRequest::kData){
                if (!opened){
                    return Status(StatusCode::CANCELLED, "Meta data must come in first.");
                }
                ofs.write(request.data().data(), request.data().size());
                if (!ofs.good()){
                    ofs.close();
                    return Status(StatusCode::CANCELLED, "Write failed.");
                }
            }
        }

        if (ofs.is_open()){
            ofs.close();
        }

        struct stat st;
        if (filename.empty() || stat(fullPath.c_str(), &st) != 0){
            return Status(StatusCode::CANCELLED, "Stored file status is unavailable.");
        }

        FileMetaData* metaData = reply->mutable_status();
        metaData->set_filename(filename);
        metaData->set_size(static_cast<uint64_t>(st.st_size));
        metaData->set_mtime(static_cast<int64_t>(st.st_mtime));
        metaData->set_crc(dfs_file_checksum(fullPath, &this->crc_table));

        std::lock_guard<std::mutex> guard(lock_m);
        file_locks.erase(filename);
        
        CountChange();
        return Status::OK;
    }

    Status Fetch(ServerContext* ctx,
                const FileRequest* request,
                ServerWriter<FileChunk>* writer) override{
        std::string fullPath = WrapPath(request->filename());
        
        struct stat st;
        if (stat(fullPath.c_str(), &st) != 0){
            return Status(StatusCode::NOT_FOUND, "File not found.");
        }

        std::ifstream ifs(fullPath, std::ios::binary);
        if (!ifs.is_open()){
            return Status(StatusCode::CANCELLED, "Unable to open file.");
        }

        char buf[2048];
        while (ifs.good()){
            ifs.read(buf, sizeof(buf));
            std::streamsize sent = ifs.gcount();
            if (sent > 0){
                FileChunk chunk;
                chunk.set_data(buf, static_cast<size_t>(sent));
                if (!writer->Write(chunk)){
                    ifs.close();
                    return Status(StatusCode::CANCELLED, "Stream write has failed.");
                }
            }
        }
        ifs.close();
        return Status::OK;
    }

    Status Delete(ServerContext* ctx,
                const FileRequest* request,
                DeleteReply* reply) override{
        std::string fullPath = WrapPath(request->filename());

        struct stat st;
        if (stat(fullPath.c_str(), &st) != 0){
            return Status(StatusCode::NOT_FOUND, "File not found.");
        }

        if (std::remove(fullPath.c_str()) != 0){
            return Status(StatusCode::CANCELLED, "Failed to delete file.");
        }

        reply->set_deleted(true);

        std::lock_guard<std::mutex> guard(lock_m);
        file_locks.erase(request->filename());
        
        CountChange();
        return Status::OK;
    }
    
    Status List(ServerContext* ctx,
                const ListRequest* request,
                ListReply* reply) override{
        FillServerFileList(reply);
        return Status::OK;
    }

    Status Stat(ServerContext* ctx,
                const FileRequest* request,
                FileStatus* response) override{
        std::string fullPath = WrapPath(request->filename());

        struct stat st;
        if (stat(fullPath.c_str(), &st) != 0){
            return Status(StatusCode::NOT_FOUND, "File not found.");
        }

        response->set_filename(request->filename());
        response->set_size(static_cast<uint64_t>(st.st_size));
        response->set_mtime(static_cast<int64_t>(st.st_mtime));
        response->set_crc(dfs_file_checksum(fullPath, &this->crc_table)); 
        return Status::OK;
    }
    //Add the mutexes
};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly
// to add additional startup/shutdown routines inside, but be aware that
// the basic structure should stay the same as the testing environment
// will be expected this structure.
//
/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        int num_async_threads,
        std::function<void()> callback) :
        server_address(server_address),
        mount_path(mount_path),
        num_async_threads(num_async_threads),
        grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);


    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional definitions here
//
