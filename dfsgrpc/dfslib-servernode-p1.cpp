#include <map>
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

#include "src/dfs-utils.h"
#include "dfslib-shared-p1.h"
#include "dfslib-servernode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

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


//
// STUDENT INSTRUCTION:
//
// DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in the `dfs-service.proto` file.
//
// You should add your definition overrides here for the specific
// methods that you created for your GRPC service protocol. The
// gRPC tutorial described in the readme is a good place to get started
// when trying to understand how to implement this class.
//
// The method signatures generated can be found in `proto-src/dfs-service.grpc.pb.h` file.
//
// Look for the following section:
//
//      class Service : public ::grpc::Service {
//
// The methods returning grpc::Status are the methods you'll want to override.
//
// In C++, you'll want to use the `override` directive as well. For example,
// if you have a service method named MyMethod that takes a MyMessageType
// and a ServerWriter, you'll want to override it similar to the following:
//
//      Status MyMethod(ServerContext* context,
//                      const MyMessageType* request,
//                      ServerWriter<MySegmentType> *writer) override {
//
//          /** code implementation here **/
//      }
//
class DFSServiceImpl final : public DFSService::Service {

private:

    /** The mount path for the server **/
    std::string mount_path;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }


public:

    DFSServiceImpl(const std::string &mount_path): mount_path(mount_path) {
    }

    ~DFSServiceImpl() {
    }

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // implementations of your protocol service methods
    //

    
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
        return Status::OK;
    }
    
    Status List(ServerContext* ctx,
                const ListRequest* request,
                ListReply* reply) override{
        DIR* directory = opendir(this->mount_path.c_str());
        if (!directory){
            return Status(StatusCode::CANCELLED, "Unable to open mount directory.");
        }

        struct dirent* entry;
        while ((entry = readdir(directory)) != nullptr){
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
            }
        }

        closedir(directory);
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
        
        return Status::OK;
    }
};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly,
// but be aware that the testing environment is expecting these three
// methods as-is.
//
/**
 * The main server node constructor
 *
 * @param server_address
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        std::function<void()> callback) :
    server_address(server_address), mount_path(mount_path), grader_callback(callback) {}

/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
    this->server->Shutdown();
}

/** Server start **/
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path);
    ServerBuilder builder;
    builder.AddListeningPort(this->server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    this->server = builder.BuildAndStart();
    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    this->server->Wait();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional DFSServerNode definitions here
//
