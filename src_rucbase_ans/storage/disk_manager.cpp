#include "storage/disk_manager.h"

#include <assert.h>    // for assert
#include <string.h>    // for memset
#include <sys/stat.h>  // for stat
#include <unistd.h>    // for lseek

#include "defs.h"

DiskManager::DiskManager() { memset(fd2pageno_, 0, MAX_FD * (sizeof(std::atomic<page_id_t>) / sizeof(char))); }

/**
 * @brief Write the contents of the specified page into disk file
 *
 */
void DiskManager::write_page(int fd, page_id_t page_no, const char *offset, int num_bytes) {
    // Todo:
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用write()函数
    // 注意处理异常
    off_t currpos = lseek(fd, page_no*num_bytes, SEEK_SET);
    if (currpos == -1) {
        throw UnixError();
    }
    write(fd, offset, num_bytes);

    return;
}

/**
 * @brief Read the contents of the specified page into the given memory area
 */
void DiskManager::read_page(int fd, page_id_t page_no, char *offset, int num_bytes) {
    // Todo:
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用read()函数
    // 注意处理异常
    off_t currpos = lseek(fd, page_no*num_bytes, SEEK_SET);
    if (currpos == -1) {
        throw UnixError();
    }
    read(fd, offset, num_bytes);

    return;
}

/**
 * @brief Allocate new page (operations like create index/table)
 * For now just keep an increasing counter
 */
page_id_t DiskManager::AllocatePage(int fd) {
    // Todo:
    // 简单的自增分配策略，指定文件的页面编号加1
    fd2pageno_[fd]++;
    return fd2pageno_[fd]-1;
}

/**
 * @brief Deallocate page (operations like drop index/table)
 * Need bitmap in header page for tracking pages
 * This does not actually need to do anything for now.
 */
void DiskManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {}

bool DiskManager::is_dir(const std::string &path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

void DiskManager::create_dir(const std::string &path) {
    // Create a subdirectory
    std::string cmd = "mkdir " + path;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为path的目录
        throw UnixError();
    }
}

void DiskManager::destroy_dir(const std::string &path) {
    std::string cmd = "rm -r " + path;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @brief 用于判断指定路径文件是否存在 
 */
bool DiskManager::is_file(const std::string &path) {
    // Todo:
    // 用struct stat获取文件信息
    struct stat s;
    if (stat(path.c_str(), &s) == 0) {
        return s.st_mode && S_IFREG;
    }

    return false;
}

/**
 * @brief 用于创建指定路径文件
 */
void DiskManager::create_file(const std::string &path) {
    // Todo:
    // 调用open()函数，使用O_CREAT模式
    // 注意不能重复创建相同文件
    bool exist = false;
    struct stat s;
    if (stat(path.c_str(), &s) == 0) {
        exist = s.st_mode && S_IFREG;
    }

    if (!exist) {
        open(path.c_str(), O_CREAT, 0600);
    }

    return;
}

/**
 * @brief 用于删除指定路径文件 
 */
void DiskManager::destroy_file(const std::string &path) {
    // Todo:
    // 调用unlink()函数
    // 注意不能删除未关闭的文件
    bool found = false; // make sure the file is closed
    if (path2fd_.find(path) != path2fd_.end())  found = true;
    
    if (found) {
        // not yet closed
        int fd = path2fd_.find(path)->second;
        path2fd_.erase(path2fd_.find(path));
        fd2path_.erase(fd2path_.find(fd));
        close(fd);
    }

    unlink(path.c_str());
    return;
}

/**
 * @brief 用于打开指定路径文件
 */
int DiskManager::open_file(const std::string &path) {
    // Todo:
    // 调用open()函数，使用O_RDWR模式
    // 注意不能重复打开相同文件，并且需要更新文件打开列表
    bool found = false;
    std::unordered_map<std::string, int>::iterator p2fIter;
    // std::unordered_map<int, std::string>::iterator f2pIter;

    for (p2fIter = path2fd_.begin(); p2fIter != path2fd_.end(); p2fIter++) {
        if (p2fIter->first == path) {
            found = true;
        }
    }

    if (found == false) {
        int fd = open(path.c_str(), O_RDWR, 0600);
        std::pair<int,std::string> tmpfp ;
        tmpfp.first = fd;   tmpfp.second = path;
        std::pair<std::string, int> tmppf;
        tmppf.first = path;    tmppf.second = fd;
        path2fd_.insert(tmppf);
        fd2path_.insert(tmpfp);
        return fd;
    }

    return -1;
}

/**
 * @brief 用于关闭指定路径文件
 */
void DiskManager::close_file(int fd) {
    // Todo:
    // 调用close()函数
    // 注意不能关闭未打开的文件，并且需要更新文件打开列表
    std::unordered_map<int, std::string>::iterator f2pIter;
    std::unordered_map<std::string, int>::iterator p2fIter;
    
    bool found = false;

    for (f2pIter = fd2path_.begin(); f2pIter != fd2path_.end(); ) {
        if (f2pIter->first == fd) {
            found = true;
            f2pIter = fd2path_.erase(f2pIter);
        }
        else f2pIter++;
    }

    for (p2fIter = path2fd_.begin(); p2fIter != path2fd_.end(); ) {
        if (p2fIter->second == fd) {
            found = true;
            p2fIter = path2fd_.erase(p2fIter);
        }
        else p2fIter++;
    }

    if (found) {
        close(fd);
    }
    else {
        throw FileNotOpenError(fd);
    }

    return;
}

int DiskManager::GetFileSize(const std::string &file_name) {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

std::string DiskManager::GetFileName(int fd) {
    if (!fd2path_.count(fd)) {
        throw FileNotOpenError(fd);
    }
    return fd2path_[fd];
}

int DiskManager::GetFileFd(const std::string &file_name) {
    if (!path2fd_.count(file_name)) {
        return open_file(file_name);
    }
    return path2fd_[file_name];
}

bool DiskManager::ReadLog(char *log_data, int size, int offset, int prev_log_end) {
    // read log file from the previous end
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }
    offset += prev_log_end;
    int file_size = GetFileSize(LOG_FILE_NAME);
    if (offset >= file_size) {
        return false;
    }

    size = std::min(size, file_size - offset);
    lseek(log_fd_, offset, SEEK_SET);
    ssize_t bytes_read = read(log_fd_, log_data, size);
    if (bytes_read != size) {
        throw UnixError();
    }
    return true;
}

void DiskManager::WriteLog(char *log_data, int size) {
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }

    // write from the file_end
    lseek(log_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_fd_, log_data, size);
    if (bytes_write != size) {
        throw UnixError();
    }
}
