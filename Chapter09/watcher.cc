#include "watcher.h"

#include <ace/Reactor.h>
#include <ace/Thread_Manager.h>

ProcMap Watcher::pMap_;

Watcher::Watcher()
{}

Watcher::~Watcher()
{
    killAllChildren();
}

void Watcher::killAllChildren()
{
    ProcMapIter it = pMap_.begin();
    for(; it != pMap_.end(); ++it)
    {
        pm_->terminate(it->first, SIGKILL);
        ACE_DEBUG((LM_ERROR,
                    "terminated %d\n", it->first));
    }
}

void Watcher::execProc(const char* cmdLine)
{
    ProcInfo_SP info;
    info.reset(new ProcInfo);
    info->opts_.command_line(cmdLine);
    pid_t pid = pm_->spawn(&(info->proc_), info->opts_);
    pm_->register_handler(this, pid);
    pMap_[pid] = info;
    ACE_DEBUG((LM_ERROR, 
                "(%t)execProc: launched %s pid %d\n",
                cmdLine, pid));
}

int Watcher::handle_exit(ACE_Process* proc)
{
    ACE_DEBUG((LM_ERROR, "***** process %d exited ***** \n", 
                proc->getpid()));
    if(DbUtils::unrelated_ == 1)
        return;
    try
    {
        int state = DbUtils::getEnv()->failchk(0);
        if(state != 0)
        {
            ACE_DEBUG((LM_ERROR,
                "handle_exit:dbenv_failchk returned %d\n",
                 state));
        }
    }
    catch(DbException &ex)
    {
        DbUtils::getEnv()->err(ex.get_errno(), 
                "failchk returned err\n");
        DbUtils::runRecovery_ = 1;
        killAllChildren();
        ACE_OS::exit(1);
    }
}

void Watcher::threadId(DbEnv *env,
                       pid_t *pid,
                       db_threadid_t *tid)
{
    ACE_thread_t id = ACE_Thread::self();
    *tid = id;
    *pid = ACE_OS::getpid();
    //ACE_DEBUG((LM_ERROR, "Watcher::threadId: pid %d\n",
    //                      ACE_OS::getpid() ));
}

int Watcher::isAlive(DbEnv* env,
                     pid_t pid,
                     db_threadid_t tid)
{
    int ret = 0;
    if(pid == ACE_OS::getpid())
        return 1;
    ProcMapIter it = pMap_.find(pid);
    if(it != pMap_.end())
    {
        ret = (it->second)->proc_.running();
    }
    else
        ret = 1;
    ACE_DEBUG((LM_ERROR, 
                "isAlive: returning %d for %d\n",
                ret, pid));
    return ret;
}

int main(int argc, char **argv)
{
    DbUtils::processNumber_ = 1;
    DbUtils::unrelated_ = 1;
    try
    {
        DbUtils_SP utils = DbUtils::getInstance();
        utils->getEnv()->set_thread_id(Watcher::threadId);
        utils->getEnv()->set_isalive(Watcher::isAlive);
        utils->loadDbs();
    }
    catch(DbException &ex)
    {
        DbUtils::getEnv()->err(ex.get_errno(), 
                               "error in main");
    }
    Watcher w;
    w.pm_ = ACE_Process_Manager::instance();
    w.pm_->open(16, ACE_Reactor::instance());
    w.execProc("./tds 4 1");
    w.execProc("./tds 3 1");
    w.execProc("./tds 5 1");
    w.execProc("./tds 2 1");
    ACE_Reactor::instance()->run_reactor_event_loop();
}
