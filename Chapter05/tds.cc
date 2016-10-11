#include <iostream>
#include <sstream>
#include <string>

#include <db_cxx.h>

#include <ace/Task.h>
#include <ace/Time_Value.h>
#include <ace/Thread_Manager.h>

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

void errCallback (const DbEnv *env, const char *prefix, const char *errMsg)
{
    ACE_DEBUG((LM_ERROR, "(%t) %s errMsg: %s\n", prefix, errMsg));
}

class CDSWorker: public ACE_Task_Base
{
    public: 
        CDSWorker(DbEnv *env, Db *db, int type, int id);
        virtual int svc();

    private:
        void runReader();
        void runNonCursorReader();
        void runWriter();
        void runCheckpoint();
        void archive(u_int32_t flag);

        DbEnv   *env_;
        Db      *db_;
        int    type_;
        int     id_;
        
};

CDSWorker::CDSWorker(DbEnv *env, Db *db, int type, int id)
    :env_(env), db_(db), type_(type), id_(id)
{}

int CDSWorker::svc()
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
            case NON_CURSOR_READER:
                runNonCursorReader();
                break;
            case CHECKPOINT:
                runCheckpoint();
                break;
            default:
                ACE_DEBUG((LM_ERROR, "unknown type %d\n", type_));
                break;
        }
    }
    catch(...)
    {
        ACE_DEBUG((LM_ERROR, "CDSWorker: exception caught\n"));
    }
}

void CDSWorker::runReader()
{
    Dbc *dbcur;
    Dbt key;
    Dbt value;

    DbTxn *txn;
    try
    {
        env_->txn_begin(NULL, &txn, 0);
        db_->cursor(txn, &dbcur, DB_DEGREE_2);
        while (dbcur->get(&key, &value, DB_NEXT) == 0)
        {
            ACE_DEBUG((LM_ERROR, "Reader %d: key \"%s\" data \"%s\"\n",
                        id_, (char*)(key.get_data()),
                        (char*)(value.get_data()) ));
        }
        dbcur->close();
        txn->commit(0);
    }
    catch(DbException &txn_ex)
    {
        txn->abort();
    }

}

void CDSWorker::runWriter()
{
    for(int i=0; i < NUM_RECS; i++)
    {
        stringstream keyss;
        keyss << id_;
        keyss << "_";
        keyss << i;
        string keystr = keyss.str();

        stringstream valss;
        valss << id_;
        valss << "_";
        valss << i*i;
        string valstr = valss.str();

        Dbt key((void*)(keystr.c_str()), keystr.size() + 1);
        char buff[6000];
        strcpy(buff, valstr.c_str());
        Dbt value((void*)(buff), 6000);

        DbTxn *txn;
        try
        {
            env_->txn_begin(NULL, &txn, 0);
            int ret = db_->put(txn, &key, &value, 0); 
            if(ret)
                env_->err(ret, keystr.c_str());
#if 0
            else
                ACE_DEBUG((LM_ERROR, "Writer %d: key \"%s\" data \"%s\" \n",
                            id_,
                            keystr.c_str(),
                            valstr.c_str() ));
#endif
            txn->commit(0);
        }
        catch(DbException &txn_ex)
        {
            env_->err(txn_ex.get_errno(), "writer");
            txn->abort();
        }
    }
}

void CDSWorker::runNonCursorReader()
{
    DbTxn *txn;
    env_->txn_begin(NULL, &txn, 0);
    while(true)
    {
        int j = 0;
        for(int i=0; i < NUM_RECS; i++)
        {
            stringstream keyss;
            keyss << "1_";
            keyss << i;
            string keystr = keyss.str();

            Dbt key((void*)(keystr.c_str()), keystr.size() + 1);
            Dbt value;
            value.set_flags(DB_DBT_MALLOC);

            try
            {
                int ret = db_->get(txn, &key, &value,0 ); 
                if(ret == DB_NOTFOUND)
                {
                    //ACE_DEBUG((LM_ERROR, "key not found\n"));
                    txn->commit(0);
                    env_->txn_begin(NULL, &txn, 0);
                    i --;
                    continue;
                }
                else if(ret != 0)
                {
                    env_->err(ret, keystr.c_str());
                }
                else
                {
#if 0
                    ACE_DEBUG((LM_ERROR, "NonCursorReader %d: key \"%s\" data \"%s\" size %d \n",
                                id_,
                                keystr.c_str(),
                                (char*)(value.get_data()), value.get_size() ));
#endif
                }
                j++;
                if(j >= 20)
                {
                    txn->commit(0);
                    env_->txn_begin(NULL, &txn, 0);
                    j = 0;
                }
            }
            catch(DbException &txn_ex)
            {
                env_->err(txn_ex.get_errno(), "nonCursorReader");
                txn->abort();
                env_->txn_begin(NULL, &txn, 0);
            }
        }
    }
}

void CDSWorker::runCheckpoint()
{
    int ARCH_INTERVAL = 60;
    ACE_Time_Value last_arch = 0;
        
    while(true)
    {
        try
        {
            env_->txn_checkpoint(0,0,0);
            ACE_Time_Value curr_time = ACE_OS::gettimeofday();
            if(curr_time.sec() - last_arch.sec() > ARCH_INTERVAL)
            {
                ACE_DEBUG((LM_ERROR, "removing unused log files\n"));
                archive(DB_ARCH_REMOVE);

                ACE_DEBUG((LM_ERROR, "archiving data files\n"));
                archive(DB_ARCH_DATA);

                ACE_DEBUG((LM_ERROR, "archiving log files\n"));
                archive(DB_ARCH_LOG);
            }
        }
        catch(DbException &ex)
        {
            env_->err(ex.get_errno(), "checkpoint");
        }
        ACE_OS::sleep(60);
    }
}

void CDSWorker::archive(u_int32_t flags)
{
    int ret;
    char **file_list = NULL;

    ret = env_->log_archive(&file_list, flags);
    if(ret)
    {
        env_->err(ret, "runCheckpoint: archive failed");
    }
    else if(file_list != NULL)
    {
        char **begin = file_list;
        for(; *file_list != NULL; ++file_list)
        {
            ACE_DEBUG((LM_ERROR, "%s\n", *file_list));
        }
        free(begin);
    }
}

int main(int argc, char **argv)
{
    DbEnv dbenv(0);
    try
    {
        const char *envhome = "./chap5_env";

        dbenv.set_errpfx("env_ex");
        dbenv.set_errcall(errCallback);
        dbenv.set_lk_detect(DB_LOCK_DEFAULT);

        u_int32_t envFlags = 
            DB_CREATE | DB_INIT_MPOOL | DB_INIT_TXN | 
            DB_INIT_LOG | DB_INIT_LOCK | DB_RECOVER | DB_THREAD;
        
        dbenv.open(envhome, envFlags, 0);

        Db db(&dbenv, 0);
        DbTxn *txn;
        try
        {
            dbenv.txn_begin(NULL, &txn, 0);
            db.set_pagesize(512);
            db.open(txn, "chap5_tds", NULL , 
                    DB_BTREE, DB_CREATE | DB_THREAD, 0644);
            txn->commit(0);
        }
        catch(DbException &txn_ex)
        {
            txn->abort();
        }

        CDSWorker *w1 = new CDSWorker(&dbenv, &db, WRITER, 1);
        if(w1->activate((THR_NEW_LWP | THR_JOINABLE), 1) != 0)
            ACE_DEBUG((LM_ERROR, "Could not launch w1\n"));

        CDSWorker *w2 = new CDSWorker(&dbenv, &db, WRITER, 1);
        if(w2->activate((THR_NEW_LWP | THR_JOINABLE), 1) != 0)
            ACE_DEBUG((LM_ERROR, "Could not launch w1\n"));
        //ACE_OS::sleep(1);

        CDSWorker *r1 = new CDSWorker(&dbenv, &db, NON_CURSOR_READER, 2);
        if(r1->activate((THR_NEW_LWP | THR_JOINABLE), 1) != 0)
            ACE_DEBUG((LM_ERROR, "Could not launch r1\n"));

        CDSWorker *r2 = new CDSWorker(&dbenv, &db, NON_CURSOR_READER, 3);
        if(r2->activate((THR_NEW_LWP | THR_JOINABLE), 1) != 0)
            ACE_DEBUG((LM_ERROR, "Could not launch r3\n"));

        CDSWorker *r3 = new CDSWorker(&dbenv, &db, NON_CURSOR_READER, 4);
        if(r3->activate((THR_NEW_LWP | THR_JOINABLE), 1) != 0)
            ACE_DEBUG((LM_ERROR, "Could not launch r4\n"));

        CDSWorker *r4 = new CDSWorker(&dbenv, &db, CHECKPOINT, 5);
        if(r4->activate((THR_NEW_LWP | THR_JOINABLE), 1) != 0)
            ACE_DEBUG((LM_ERROR, "Could not launch r5\n"));

        ACE_Thread_Manager *tm = ACE_Thread_Manager::instance();
        tm->wait();

        delete r1;
        delete w1;
        delete r2;
        delete r3;
        delete r4;

        db.close(0);
        dbenv.close(0);

    }
    catch(DbException &dbex)
    {
        dbenv.err(dbex.get_errno(), "Db exception caught");
    }
    catch(...)
    {
        cout << "unknown exception caught" << endl;
    }
}
