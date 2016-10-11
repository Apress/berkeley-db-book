#include "worker.h"

using std::cout;
using std::endl;
using std::cerr;
using std::stringstream;
using std::string;


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
    while(true)
    {
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
        ACE_OS::sleep(5);
    }
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
