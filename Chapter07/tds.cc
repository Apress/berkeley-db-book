#include <iostream>
#include <sstream>
#include <string>

#include <ace/Task.h>
#include <ace/Time_Value.h>
#include <ace/Thread_Manager.h>
#include <ace/OS_NS_Thread.h>

#include <boost/scoped_ptr.hpp>

#include "load_db.h"

using std::cout;
using std::endl;
using std::cerr;
using std::stringstream;
using std::string;

#define NUM_RECS 50000

#define READER 1
#define WRITER 2
#define NON_CURSOR_READER 3
#define CHECKPOINT 4

class DbWorker;
typedef boost::scoped_ptr<DbWorker> DbWorker_SP;

class DbWorker: public ACE_Task_Base
{
    public: 
        DbWorker(int type, int id);
        virtual int svc();
    private:
        void runReader();
        void runWriter();

        int    type_;
        int     id_;
        
};

DbWorker::DbWorker(int type, int id)
    :type_(type), id_(id)
{}

int DbWorker::svc()
{
    try
    {
        switch(type_)
        {
            case READER:
                runReader();
                break;
            case WRITER:
                runWriter();
                break;
            default:
                ACE_DEBUG((LM_ERROR, "unknown type %d\n", type_));
                break;
        }
    }
    catch(...)
    {
        ACE_DEBUG((LM_ERROR, "DbWorker: exception caught\n"));
    }
}

void DbWorker::runReader()
{
    ACE_DEBUG((LM_ERROR, 
                "(%P:%t)runReader started\n"));
    PersonIter *iter = new PersonIter();
    while(iter->next())
    {
        Person p = iter->getEntry();
        ACE_DEBUG((LM_ERROR, 
                    "(%P:%t)runReader:ssn %u: name %s dob %s\n",
                    p.getSSN(),
                    p.getName().c_str(),
                    p.getDOB().c_str()));
        if(DbUtils::processNumber_ == 4)
            ACE_OS::exit(1);
    }
    iter->close();
    delete iter;
    ACE_DEBUG((LM_ERROR, "(%P:%t)runReader: finished\n"));
}

void DbWorker::runWriter()
{
    ACE_DEBUG((LM_ERROR, 
                "(%P:%t)runWriter started\n"));
    while(true)
    {
        Person p;
        unsigned long ss = 111223334;
        DbTxn *t;
        try
        {
            DbUtils::getEnv()->txn_begin(NULL, &t, 0);
            if( p.getBySSN(ss, t) == 0)
            {
                p.setDOB("jan 1 2001");
                p.insert(t);
                t->commit(0);
                ACE_DEBUG((LM_ERROR,
                            "(%P:%t)runWriter: insert done\n"));
            }
        }
        catch(DbException& ex)
        {
            DbUtils::getEnv()->err(ex.get_errno(), 
                    "DbEntry::dbPut");
            t->abort();
        }
        ACE_OS::sleep(1);
    }
}


int main(int argc, char **argv)
{
    if (argc > 1)
    {
        DbUtils::processNumber_ = atoi(argv[1]);
        ACE_DEBUG((LM_ERROR, 
                    "(%P:%t)tds:main: process number %d\n",
                    DbUtils::processNumber_));
    }
    else
    {
        ACE_DEBUG((LM_ERROR, "(%P:%t)tds:main: too few args\n"));
        return 1;
    }

    try
    {
        DbUtils_SP utils = DbUtils::getInstance();
        utils->setMainThId(ACE_Thread::self());
        utils->loadDbs();

        DbWorker_SP w1(new DbWorker( WRITER, 1));
        if(w1->activate((THR_NEW_LWP | THR_JOINABLE), 1) != 0)
            ACE_DEBUG((LM_ERROR, "Could not launch w1\n"));

        DbWorker_SP r1(new DbWorker( READER, 2));
        if(r1->activate((THR_NEW_LWP | THR_JOINABLE), 1) != 0)
            ACE_DEBUG((LM_ERROR, "Could not launch r1\n"));

        ACE_Thread_Manager *tm = 
            ACE_Thread_Manager::instance();
        tm->wait();
    }
    catch(DbException& dbex)
    {
        ACE_DEBUG((LM_ERROR, 
                    "caught DbException in main %d\n",
                    dbex.get_errno()));
    }
    catch(...)
    {
        ACE_DEBUG((LM_ERROR, "unknown exception caught\n"));
    }
}
