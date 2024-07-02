#define _POSIX_SOURCE
#define __STDC_WANT_LIB_EXT2__ 1
#define _XOPEN_SOURCE 700
#define _DEFAULT_SOURCE

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <unistd.h>

#if !defined(__GLIBC__) && _POSIX_C_SOURCE < 200809L
#error This program requires POSIX 2008 or newer
#endif

#define MODULE_DIRECTORY "/usr/local/lighting/modules"
#define MAX_MODULES 32
#define MAX_MODULE_NAME (PATH_MAX - sizeof(MODULE_DIRECTORY))
#define COMM_FILES "/tmp/lighting/comm.txt"
#define ONE_BILLION_DOLLARS 1000000000
#define ONE_MILLION_DOLLARS 1000000
#define CLOCK_TO_USE CLOCK_MONOTONIC

static const char *commFile = COMM_FILES;
static const char *requiredModules[] = {"display", "clear"};
static const uint16_t requiredModulesLength = sizeof(requiredModules) / sizeof(char *);

bool timeoutFlag = true;
bool listening = true;
bool end = false;
uint8_t nowFlag = 0;
uint8_t nextJob = 0;
uint8_t addIndex = 0;

pthread_t listenerThread;
pthread_t thread;

struct Job *jobs[32] = {NULL};
struct Module *modules[MAX_MODULES + sizeof(requiredModules) / sizeof(char *)] = {0};

// Util Functions

static inline char *rstrchr(const char *str, char c) {
  int i = strlen(str);
  while (i-- > 0) if (str[i] == c) return (char *) (str + i);
  return NULL;
}

static inline int rmkdir(const char *path, mode_t mode) {
  char *cpath, *lastSlash;
  int res;

  if (path[0] == 0) return -1;

  if ((res = mkdir(path, mode)) == 0) {
    return 0;
  }

  if (errno == 2) {
    if ((lastSlash = rstrchr(path, '/')) == NULL) {
      return -1;
    }

    cpath = strndup(path, (size_t) (lastSlash - path));
    printf("Creating %s\n", cpath);
    res = rmkdir(cpath, mode);
    free(cpath);

    if (res == -1 && errno != EEXIST) {
      return -1;
    }

    return mkdir(path, mode);
  }

  return -1;
}

static inline int strew(const char *string, const char *end) {
    char *startOfEnd = (char *) ((long long int) string + strlen(string) - strlen(end));
    return strcmp(startOfEnd, end);
}

static inline char *rmext(const char *fname) {
    return strndup(fname, (int64_t)rstrchr(fname, '.') - (int64_t)fname);
}

enum ModuleType {
    EXE,
    PY,
};

// Make sure low is lowest and high is highest
enum Priority {
    LOW,
    MID,
    HIGH,
    NOW,
};

enum Comm {
    OK = 0x01,
    STOP,
    STOP_CURRENT_JOB,
    GETPID,
    ALIVE,
    NOT_FOUND
};

enum Command {
    EXEC = 0x15,
    DISPLAY,
    CLEAR,
};

struct Module {
    const char moduleName[MAX_MODULE_NAME + 1];
    enum ModuleType type;
};

struct Job {
    enum Priority priority;
    const char *cmd;
    uint64_t timeLimit;
};

struct Timeout {
    bool *timeoutFlag;
    uint64_t timeout;
};

// Scheduler
bool startScheduler();
void stopScheduler();
int pushJob(struct Job *);
struct Job *getNextJob();
void freeJob(struct Job *job);

// Executer
bool startExecuter();
void stopExecuter();

// Comm
bool startListener();
void stopListener();
int messageDaemon(const char *message, char *response, ssize_t rsize);

// Modules
int loadModules();
void freeModules();
const struct Module *getModule(uint8_t index);

static inline bool start() {
    return
    loadModules() != -1
    && startScheduler()
    && startExecuter()
    && startListener()
    ;
}

static inline void stop() {
    stopListener();
    stopScheduler();
    stopExecuter();
    exit(0);
}

// Comms

void *timeoutThreadFunction(void *arg) {
    struct Timeout *to = (struct Timeout *) arg;
    *to->timeoutFlag = false;
    usleep((uint64_t) to->timeout);
    *to->timeoutFlag = true;
    return NULL;
}

int messageDaemon(const char *message, char *response, ssize_t rsize) {
    int f, r;
    bool timeoutFlag = false;
    pthread_t tid;

    f = open(commFile, O_APPEND | O_RDWR | O_SYNC);

    if (f == -1) {
        // printf("Failed to open comms files, errno: %d\n", errno);
        return -1;
    }

    write(f, message, strlen(message));
    // Read will return the new data according to posix
    struct Timeout to = { &timeoutFlag, 3e6 };
    pthread_create(&tid, NULL, timeoutThreadFunction, &to);
    pthread_detach(tid);
    r = read(f, response, rsize);
    memset(response, 0, rsize);
    // Todo add timeout
    while ((r = read(f, response, rsize)) <= 0) {
        if (timeoutFlag) return -5;
    }

    close(f);

    return 1;
}

void *listener(void *args) {
    int f;
    char buffer[129] = {0};
    ssize_t r;
    struct Job job;

    mode_t t = umask(0);
    f = open(commFile, O_TRUNC | O_RDWR | O_SYNC | O_CREAT, 0666);
    umask(t);
    if (f < 0) {
        printf("Failed to open Comms File, please ensure '%s' exists and is accessible\n", commFile);
        return NULL;
    }

    write(f, "Hello World\n", 13);
    read(f, NULL, 0);

    while (listening) {
        r = read(f, buffer, sizeof(buffer));

        if (r > 0) {
            switch (buffer[0]) {
                case EXEC: {
                    memset(&job, 0, sizeof(struct Job));

                    uint8_t moduleIndex = buffer[2] - '0';
                    char cmd[256] = {0};
                    const struct Module *module = getModule(moduleIndex);

                    if (module == NULL) {
                        dprintf(f, "%c\n", NOT_FOUND);
                        break;
                    }

                    if (module->type == PY) {
                        snprintf(cmd, sizeof(cmd), "python3 %s/%s.py", MODULE_DIRECTORY, module->moduleName);
                    } else {
                        snprintf(cmd, sizeof(cmd), "%s/%s.sh", MODULE_DIRECTORY, module->moduleName);
                    }

                    job.cmd = cmd;
                    job.priority = buffer[2] - '0';
                    job.timeLimit = atoi(buffer + 5);

                    r = dprintf(f, "%c%c\n", OK, pushJob(&job));
                    break;
                }

                case ALIVE: {
                    r = dprintf(f, "%c\n", ALIVE);
                    break;
                }

                case GETPID: {
                    r = dprintf(f, "%d\n", getpid());
                    break;
                }

                case STOP: {
                    r = dprintf(f, "%c\n", OK);
                    stop();
                    break;
                }

                default: {
                    r = dprintf(f, "%d Command 0x%x not found\n", NOT_FOUND, buffer[0]);
                    read(f, NULL, 0);
                    break;
                }
            }

            memset(buffer, 0, sizeof(buffer));
        }

        // TODO: adjust this
        sleep(1);
    }

    close(f);

    return NULL;
}

bool startListener() {
    printf("Starting listener\n");
    char *parentFolderPath = strndup(commFile, (size_t) rstrchr(commFile, '/') - (size_t) commFile);
    
    if (rmkdir(parentFolderPath, 0777) == -1 && errno != EEXIST) {
        printf("Error making %s, errno: %d\n", parentFolderPath, errno);
        return false;
    }

    if (chmod(parentFolderPath, 0777) == -1) {
        printf("Error changing permissions on %s, errno: %d\n", parentFolderPath, errno);
        return false;
    }

    free(parentFolderPath);

    pthread_create(&listenerThread, NULL, listener, NULL);
    printf("Started listener\n");
    return true;
}

void stopListener() {
    listening = false;
    pthread_join(listenerThread, NULL);
    printf("Stopped Listener\n");
}

// Executer 

// TODO, test possible runtime checks to increase speed
static inline bool isTimeGreater(struct timespec t0, struct timespec t1) {
    return (t0.tv_sec != t1.tv_sec) ? (t0.tv_sec > t1.tv_sec) : ((t0.tv_sec * ONE_BILLION_DOLLARS) + t0.tv_nsec) > ((t1.tv_sec * ONE_BILLION_DOLLARS) + t1.tv_nsec);
}

void *executer(void *args) {
    // executer? I barely know her
    uint64_t i = 0;
    struct Job * sJob;
    int nfd;

    while (!end) {
        sJob = getNextJob();

        if (sJob != NULL) {
            int pid, result;
            struct timespec startTime, currentTime, endTime;
            
            clock_gettime(CLOCK_TO_USE, &startTime);
            memcpy(&endTime, &startTime, sizeof(startTime));
            
            if (sJob->timeLimit > 0) {
                endTime.tv_sec += sJob->timeLimit / 1000;
                endTime.tv_nsec += (sJob->timeLimit % 1000) * 1000000;
            }

            if ((pid = fork()) == -1) {
                // cry
                freeJob(sJob);
                return NULL;
            }

            if (pid == 0) {
                setpgid(0, 0);

                nfd = open("/dev/null", O_WRONLY);
                
                if (nfd < 0) {
                    fprintf(stderr, "Failed to open /dev/null, the next few lines may be a little messy\n");
                } else {
                    if (dup2(nfd, 1) == -1) {
                        fprintf(stderr, "Failed to redirect stdout, the next few lines may be a little messy\n");
                    }

                    if (dup2(nfd, 2) == -1) {
                        fprintf(stderr, "Failed to redirect stderr, the next few lines may be a little messy\n");
                    }
                }

                result = execlp("/bin/sh", "sh", "-c", sJob->cmd, NULL);

                if (result != 0) {
                    printf("Command failed\nret:%d\nerrno:%d\n\n", result, errno);
                }

                // Kill myself
                killpg(0, SIGKILL);
                return NULL;
            } else {
                usleep(300);
                while (clock_gettime(CLOCK_TO_USE, &currentTime), isTimeGreater(endTime, currentTime)) {
                    if (waitpid(pid, NULL, WNOHANG) != 0 || nowFlag != 0) break;
                    usleep(250000);
                }

                // Kill the child
                killpg(pid, SIGKILL);

                i++;
                freeJob(sJob);
                sJob = NULL;
            }

        }

        sleep(1);
    }

    return (void *) i;
}

bool startExecuter() {
    printf("Starting Executer\n");
    pthread_create(&thread, NULL, executer, NULL);
    printf("Started Executer\n");
    return true;
}

void stopExecuter() {
    end = true;
    long int result;
    pthread_join(thread, (void **) &result);
    printf("Stopped Executer, successfully ran %ld jobs\n", result);
}

// Scheduler
bool startScheduler() {
    printf("Starting Scheduler\n");
    memset(&nextJob, 0, sizeof(nextJob));
    printf("Started Scheduler\n");
    return true;
}

void stopScheduler() {
}

int pushJob(struct Job *job) {
    uint32_t queueNum;
    jobs[addIndex] = malloc(sizeof(struct Job));
    memcpy(jobs[addIndex], job, sizeof(struct Job));
    queueNum = abs(addIndex - nextJob);
    jobs[addIndex++]->cmd = strndup(job->cmd, (size_t) 129);
    addIndex %= (sizeof(jobs) / sizeof(struct Job *));
    return queueNum;
}

struct Job *getNextJob() {
    if (jobs[nextJob] != NULL) {
        return jobs[nextJob++];
    }

    return NULL;
}

void freeJob(struct Job *job) {
    free((void *) job->cmd);
    free(job);
}

// Modules

int loadModules() {
    int i, j;
    bool required = false;
    DIR *moduleDir = NULL;
    struct dirent *f;
    printf("Loading modules\n");

    i = 0;
    while (i < 3) {
        moduleDir = opendir(MODULE_DIRECTORY);

        if (moduleDir == NULL) {
            if (errno == ENOENT) {
                if (rmkdir(MODULE_DIRECTORY, 0777) == -1) {
                    if (errno == EACCES || errno == EPERM) {
                        printf("This program doesn't have permissions to create %s, please run this program as root or run the following command\n", MODULE_DIRECTORY);
                        printf("sudo mkdir -p %s && sudo chmod 775 %s\n", MODULE_DIRECTORY, MODULE_DIRECTORY);
                        break;
                    }

                    printf("Failed to create modules directory (try %d, errno: %d)\n", ++i, errno);
                } else {
                    printf("Successfully created %s\n", MODULE_DIRECTORY);
                }

                continue;
            } else {
                printf("Failed to access directory, errno: %d\n", errno);
            }
        }

        break;
    }

    if (moduleDir == NULL) {
        printf("Failed to open modules directory\n");

        return -1;
    }

    i = requiredModulesLength;
    while ((f = readdir(moduleDir)) != NULL) {
        required = false;
        struct Module *module = malloc(sizeof(struct Module));

        if (f->d_type != 0x08) {
            continue;
        }

        char *fnameWOExt = rmext(f->d_name);
        memcpy((void *) module->moduleName, fnameWOExt, sizeof(module->moduleName) - 1);
        printf("loading module '%s'\n", fnameWOExt);
        free(fnameWOExt);

        if (strew(f->d_name, ".py") == 0) {
            module->type = PY;
        } else {
            module->type = EXE;
        }

        // Move this out of the loop
        for (j = 0; j < requiredModulesLength; j++) {
            if (strcmp(requiredModules[j], module->moduleName) == 0) {
                modules[j] = module;
                printf("Loaded module '%s' as module number %d\n", module->moduleName, j);
                required = true;
                break;
            }
        }

        if (!required) {
            printf("Loaded module '%s' as module number %d\n", module->moduleName, i);
            modules[i++] = module;
        }
    }
    
    for (i = 0; i < requiredModulesLength; i++) {
        if (modules[i] == NULL) {
            printf("Failed to load required '%s' module\n", requiredModules[i]);
            freeModules();
            return -1;
        }
    }

    return 0;
}

void freeModules() {
    int i;
    for (i = 0; i < sizeof(modules) / sizeof(struct Module *); i++) {
        if (modules[i] != NULL) {
            free(modules[i]);
            modules[i] = NULL;
        }
    }
}

const struct Module *getModule(uint8_t index) {
    return modules[index];
}

int main(int argc, char **argv) {
    char buffer[128] = {0};
    int pid, spid, moduleIndex, modulePriority = MID, res;
    uint32_t moduleTimeout = 0;


    if (argc < 2) {
        printf("Not enough Args\n");
        return -1;
    }

    if (strcmp(argv[1], "start") == 0) {
        printf("Attempting to start Daemon\n");
        if ((res = messageDaemon("\x05\n", buffer, sizeof(buffer))) != -5 && buffer[0] == ALIVE) {
            printf("Daemon already up\n");
            return 1;
        }

        if ((pid = fork()) == -1) {
            printf("Failed to fork\n");
            return -1;
        }

        if (pid == 0) {
            setsid();
            setpgid(0, 0);
            if ((spid = fork()) == -1) {
                printf("Fork Failed\n");
                return -1;
            }
            
            if (spid == 0) {
                chdir("/");
                if (!start()) {
                    printf("Failed to start\n");
                    exit(-2);
                }

                while (1);
            } else {
                // Parent of second fork
                return 0;
            }
        } else {
            // wait for Daemon to start and list all loaded modules
            return 0;
        }
    } else if (strcmp(argv[1], "stop") == 0) {
        buffer[0] = STOP;
        res = messageDaemon(buffer, buffer, sizeof(buffer));

        if (res != 1) {
            printf("Failed to message Daemon\n");
            return -1;
        }

        if (buffer[0] != OK) {
            // try harder
            memset(buffer, 0, sizeof(buffer));
            buffer[0] = GETPID;
            messageDaemon(buffer, buffer, sizeof(buffer));

            if (buffer[0] != OK) {
                printf("Failed to stop daemon, I'll try harder\n");
                res = kill(atoi(buffer), SIGKILL);

                if (res == -1) {
                    printf("Failed to stop daemon, maybe try holy water?\n");
                    return -1;
                }
            }
        }

        printf("Daemon stopped\n");
        return 0;
    } else if (strcmp(argv[1], "module") == 0) {
        // Args: lightd module <moduleIndex or "list"> [priority] [timeLimit]
        if (argc < 3) {
            printf("Not enough args\n");
            return -1;
        }

        if (strcmp(argv[2], "list") == 0) {
            // list all the modules

        } else {
            moduleIndex = atoi(argv[2]);
            if (argc >= 4) modulePriority = (uint64_t) argv[3] % (NOW + 1);
            if (argc >= 5) moduleTimeout = (uint64_t) atoi(argv[4]);

            sprintf(buffer, "%c %d %d %d\n", EXEC, moduleIndex, modulePriority, moduleTimeout);
            res = messageDaemon(buffer, buffer, sizeof(buffer));
            if (buffer[0] != OK) {
                // cry
                printf("Job failed, daemon responded with %d %d %d\n", buffer[0], buffer[1], buffer[2]);
                return -1;
            } else {
                if (buffer[1] == 0) {
                    printf("Executing module %d now\n", moduleIndex);
                } else {
                    printf("Module %d is #%d in queue\n", moduleIndex, buffer[1]);
                }

                return 0;
            }
        }
    } else {
        printf("Command '%s' not found\n", argv[1]);
    }

    return 0;
}