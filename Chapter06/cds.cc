#include <iostream>
#include <sstream>
#include <string>

#include <db_cxx.h>

#include <ace/Task.h>

using std::cout;
using std::endl;
using std::cerr;
using std::stringstream;
using std::string;

#define NUM_RECS 5 

void errCallback (const DbEnv *env, const char *prefix, const char *errMsg)
{
    ACE_DEBUG((LM_ERROR, "(%t) %s errMsg: %s\n", prefix, errMsg));
}

class CDSWorker: public ACE_Task_Base
{
    public: 
        CDSWorker(DbEnv *env, Db *db, bool reader, int id);
        virtual int svc();

    private:
        void runReader();
        void runWriter();

        DbEnv   *env_;
        Db      *db_;
        bool    reader_;
        int     id_;
};

CDSWorker::CDSWorker(DbEnv *env, Db *db, bool reader, int id)
    :env_(env), db_(db), reader_(reader), id_(id)
{}

int CDSWorker::svc()
{
    try
    {
        if(reader_)
            runReader();
        else
            runWriter();
    }
    catch(...)
    {
        ACE_DEBUG((LM_ERROR, "CDSWorker: exception caught\n"));
    }
}

void CDSWorker::runReader()
{
    Dbc *dbcur;
    db_->cursor(NULL, &dbcur, 0);
    Dbt key;
    Dbt value;

    while (dbcur->get(&key, &value, DB_NEXT) == 0)
    {
        ACE_DEBUG((LM_ERROR, "Reader %d: key \"%s\" data \"%s\"\n",
                    id_, (char*)(key.get_data()),
                    (char*)(value.get_data()) ));
        ACE_OS::sleep(1);
    }
    dbcur->close();
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
        Dbt value((void*)(valstr.c_str()), valstr.size() + 1);

        int ret = db_->put(0, &key, &value, 0); 
        if(ret)
            env_->err(ret, keystr.c_str());
        else
            ACE_DEBUG((LM_ERROR, "Writer %d: key \"%s\" data \"%s\" \n",
                        id_,
                        keystr.c_str(),
                        valstr.c_str() ));
        ACE_OS::sleep(1);
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

        dbenv.open(envhome, DB_CREATE | DB_INIT_MPOOL | DB_INIT_CDB | DB_THREAD, 0);

        Db db_ver(NULL, 0);
        int verify = db_ver.verify( "./chap5_env/chap5_cds", NULL, NULL, 0);

        ACE_DEBUG((LM_ERROR, "verify returned %d\n", verify));
            
        Db db(&dbenv, 0);
        db.open(NULL, "chap5_cds", NULL , DB_BTREE, DB_CREATE | DB_THREAD, 0644);

        CDSWorker *w1 = new CDSWorker(&dbenv, &db, false, 1);
        if(w1->activate((THR_NEW_LWP | THR_JOINABLE), 1) != 0)
            ACE_DEBUG((LM_ERROR, "Could not launch w1\n"));

        CDSWorker *r1 = new CDSWorker(&dbenv, &db, true, 2);
        if(r1->activate((THR_NEW_LWP | THR_JOINABLE), 1) != 0)
            ACE_DEBUG((LM_ERROR, "Could not launch r1\n"));

        w1->wait();
        r1->wait();

        delete r1;
        delete w1;

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
