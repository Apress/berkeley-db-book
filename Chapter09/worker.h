#ifndef _WORKER_H_
#define _WORKER_H_

#include <iostream>
#include <sstream>
#include <string>

#include <ace/Task.h>
#include <ace/Time_Value.h>
#include <ace/Thread_Manager.h>
#include <ace/OS_NS_Thread.h>

#include <boost/scoped_ptr.hpp>

#include "load_db.h"

#define READER 1
#define WRITER 2
#define NON_CURSOR_READER 3

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
#endif //_WORKER_H_
