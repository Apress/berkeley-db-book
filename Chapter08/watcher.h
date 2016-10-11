#ifndef _WATCHER_H_
#define _WATCHER_H_

#include <ace/Event_Handler.h>
#include <ace/Process.h>
#include <ace/Process_Manager.h>

#include <boost/shared_ptr.hpp>

#include <map>

#include "load_db.h"
struct ProcInfo
{
    ACE_Process_Options opts_;
    ACE_Process proc_;
};
typedef boost::shared_ptr< ProcInfo > ProcInfo_SP;
typedef std::map< pid_t, ProcInfo_SP > ProcMap;
typedef std::map< pid_t, ProcInfo_SP >::iterator ProcMapIter;

class Watcher : 
    public ACE_Event_Handler
{
    public:
        Watcher();
        ~Watcher();

        void execProc(const char* cmdLine);

        virtual int handle_exit(ACE_Process* proc);
        static void threadId(DbEnv *env,
                             pid_t *pid,
                             db_threadid_t *tid);
        static int isAlive(DbEnv* env,
                           pid_t pid,
                           db_threadid_t tid);

        ACE_Process_Manager *pm_;
    private:
        void killAllChildren();
        static ProcMap pMap_;
};
#endif //_WATCHER_H_
