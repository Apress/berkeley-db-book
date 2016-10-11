#include <iostream>
#include <sstream>
#include <string>

#include <ace/Task.h>
#include <ace/Time_Value.h>
#include <ace/Thread_Manager.h>
#include <ace/OS_NS_Thread.h>

#include <boost/scoped_ptr.hpp>

#include "load_db.h"
#include "rep.h"

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

int main(int argc, char **argv)
{
    DbUtils::repId_ = atoi(argv[1]);
    try
    {
        DbUtils_SP utils = DbUtils::getInstance();

        RepMgr *repMgr = RepMgrSingleton::instance();
        repMgr->beginRep(DbUtils::repId_);
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
