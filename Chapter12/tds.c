#include <stdio.h>
#include <string.h>
#include "db.h"

typedef struct
{
    int ssn;
    char name[64];
    char dob[16];
} person;

person records[] =
{
    {111223333, "Manish Pandey", "jan 13 1971"},
    {111223334, "Shirish Rai", "dec 23 1974"},
    {111223335, "Roland Hendel", "feb 13 1973"}
};

int num_records = 3;

DB_ENV *env;
char *env_home = "./chap12_env";

DB *person_db;
DB *person_by_name_db;

int open_env();
int open_db();
int load_db();
int dump_db();
void close_env();
int get_sec_key(DB *db, const DBT *pkey,
        const DBT *pdata, DBT *skey);
int read_rec();
int update_rec();
int delete_rec();

int open_env()
{
    int retval = 0;
    retval = db_env_create(&env, 0);
    if(retval != 0)
    {
        printf("Error creating DB_ENV handle: err: %d\n",
                retval);
        return -1;
    }
    env->set_errpfx(env, "chap_12_tds");
    env->set_data_dir(env, "./");
    env->set_cachesize(env, 0, 5000000, 1);

    int envflags = DB_CREATE | DB_INIT_LOCK |
                   DB_INIT_LOG | DB_INIT_MPOOL |
                   DB_INIT_TXN | DB_RECOVER;
    retval = env->open(env, env_home, envflags, 0);
    if(retval != 0)
    {
        env->err(env, retval,
                "Error opening the environment");
        env->close(env, 0);
        return -1;
    }
    env->set_flags(env, DB_LOG_AUTOREMOVE, 1);
    return 0;
}

int open_db()
{
    int retval = 0;

    //open the primary database
    retval = db_create(&person_db, env, 0);
    if(retval != 0)
    {
        env->err(env, retval,
                "Error creating DB handle");
        return -1;
    }
    person_db->set_errpfx(person_db, "chap_12_tds:person_db");

    int dbflags = DB_CREATE | DB_AUTO_COMMIT;
    retval = person_db->open(person_db, NULL, "person_db",
                             NULL, DB_BTREE, dbflags, 0);
    if(retval != 0)
    {
        person_db->err(person_db, retval,
                       "Error opening person_db");
        return -1;
    }

    //open the secondary database
    retval = db_create(&person_by_name_db, env, 0);
    if(retval != 0)
    {
        env->err(env, retval,
                "Error creating DB handle");
        return -1;
    }
    person_by_name_db->set_errpfx(person_by_name_db, 
            "chap_12_tds:person_by_name_db");
    person_by_name_db->set_flags(person_by_name_db, DB_DUPSORT);

    retval = person_by_name_db->open(person_by_name_db, NULL, 
            "person_by_name_db", NULL, DB_BTREE, dbflags, 0);
    if(retval != 0)
    {
        person_by_name_db->err(person_by_name_db, retval,
                       "Error opening person_db");
        return -1;
    }
    retval = person_db->associate(person_db, NULL,
            person_by_name_db, get_sec_key, 0);
    if(retval != 0)
    {
        person_db->err(person_db, retval,
                       "Error associating person_by_name_db");
        return -1;
    }
    return 0;
}

int get_sec_key(DB *db, const DBT *pkey,
        const DBT *pdata, DBT *skey)
{
    person *p = (person*)(pdata->data); 
    skey->data = p->name;
    skey->size = sizeof(p->name);
    return 0;
}

int load_db()
{
    int retval = 0;
    DBT key;
    DBT value;

    DB_TXN *txn;
    retval = env->txn_begin(env, NULL, &txn, 0);
    if(retval != 0)
    {
        env->err(env, retval,
                "error in load_db::txn_begin");
        return -1;
    }
    int i;
    for(i=0; i < num_records; i++)
    {
        person p = records[i];
        printf("inserting: ssn: %d, name: %s, dob: %s\n",
                p.ssn, p.name, p.dob);

        memset(&key, 0, sizeof(key));
        memset(&value, 0, sizeof(value));
        key.data = &(p.ssn);
        key.size = sizeof(int);

        value.data = &p;
        value.size = sizeof(person);

        retval = person_db->put(person_db, txn, 
                &key, &value, 0);
        switch(retval)
        {
            case 0:
                printf("put successful\n");
                continue;
            case DB_KEYEXIST:
                person_db->err(person_db, retval,
                        "%d already exists\n", records[i].ssn);
                break;
            default:
                person_db->err(person_db, retval,
                        "error while inserting %d error %d", 
                        records[i].ssn, retval);
                break;
        }
        retval = txn->abort(txn);
        if(retval != 0)
        {
            env->err(env, retval,
                    "error while aborting transaction");
            return -1;
        }

    }
    retval = txn->commit(txn, 0);
    if(retval != 0)
    {
        env->err(env, retval,
                "error while committing transaction");
        return -1;
    }
    return 0;
}

int update_rec()
{
    int retval = 0;
    int ssn = 111223335;
    DBT key;
    DBT value;
    memset(&key, 0, sizeof(key));
    memset(&key, 0, sizeof(value));
    key.data = &ssn;
    key.size = sizeof(ssn);

    DB_TXN *txn;
    retval = env->txn_begin(env, NULL, &txn, 0);
    if(retval != 0)
    {
        env->err(env, retval,
                "error in load_db::txn_begin");
        return -1;
    }

    retval = person_db->get(person_db, txn, 
            &key, &value, 0);
    if(retval != 0)
    {
        person_db->err(person_db, retval, "error in get");
        return -1;
        retval = txn->abort(txn);
        if(retval != 0)
        {
            env->err(env, retval,
                    "error while aborting transaction");
            return -1;
        }
    }
    person *p = (person*)(value.data);
    char *new_name = "Roland Tembo Hendle";
    memcpy(p->name, new_name, strlen(new_name)+1);

    retval = person_db->put(person_db, txn, 
            &key, &value, 0);
    if(retval != 0)
    {
        person_db->err(person_db, retval, "error in put");
        return -1;
        retval = txn->abort(txn);
        if(retval != 0)
        {
            env->err(env, retval,
                    "error while aborting transaction");
            return -1;
        }
    }
    retval = txn->commit(txn, 0);
    if(retval != 0)
    {
        env->err(env, retval,
                "error while committing transaction");
        return -1;
    }
    return 0;
}
int read_rec()
{
    int retval = 0;
    int ssn = 111223333;
    DBT key;
    DBT value;
    memset(&key, 0, sizeof(key));
    memset(&key, 0, sizeof(value));
    key.data = &ssn;
    key.size = sizeof(ssn);

    DB_TXN *txn;
    retval = env->txn_begin(env, NULL, &txn, 0);
    if(retval != 0)
    {
        env->err(env, retval,
                "error in load_db::txn_begin");
        return -1;
    }

    retval = person_db->get(person_db, txn, 
            &key, &value, 0);
    if(retval != 0)
    {
        person_db->err(person_db, retval, "error in get");
        return -1;
    }

    retval = txn->commit(txn, 0);
    if(retval != 0)
    {
        env->err(env, retval,
                "error while committing transaction");
        return -1;
    }
    person p = *((person*)(value.data));

    printf("Single read: Found for ssn: %d: name %s, dob %s\n",
            ssn, p.name, p.dob);
    return 0;
}
int delete_rec()
{
    int retval = 0;
    DB_TXN *txn;
    retval = env->txn_begin(env, NULL, &txn, 0);
    if(retval != 0)
    {
        env->err(env, retval,
                "error in delete_rec::txn_begin");
        return -1;
    }
    int ssn = 111223333;
    DBT key;
    memset(&key, 0, sizeof(key));
    key.data = &ssn;
    key.size = sizeof(ssn);

    retval = person_db->del(person_db, txn, 
            &key, 0);
    if(retval != 0)
    {
        person_db->err(person_db, retval,
                "error in delete_rec::del");

        retval = txn->abort(txn);
        if(retval != 0)
        {
            env->err(env, retval,
                    "error while aborting transaction");
            return -1;
        }
        return -1;
    }

    retval = txn->commit(txn, 0);
    if(retval != 0)
    {
        env->err(env, retval,
                "error while committing transaction");
        return -1;
    }
    return 0;
}

int dump_db()
{
    DBT key, value;
    DBC *cur;
    int retval = 0;

    DB_TXN *txn;
    retval = env->txn_begin(env, NULL, &txn, 0);
    if(retval != 0)
    {
        env->err(env, retval,
                "error in dump_db::txn_begin");
        return -1;
    }

    retval = person_db->cursor(person_db, txn, 
            &cur, 0);
    if(retval != 0)
    {
        person_db->err(person_db, retval,
                "error while opening cursor");
    }
    memset(&key, 0, sizeof(key));
    memset(&value, 0, sizeof(value));

    int ssn;
    person p;

    while(!(retval = 
                cur->c_get(cur, &key, &value, DB_NEXT)))
    {
        ssn = *((int*)(key.data));
        p = *((person*)(value.data));
        printf("Found - ssn: %d, name %s, dob %s\n",
                ssn, p.name, p.dob);
    }

    retval = cur->c_close(cur);
    if(retval != 0)
    {
        person_db->err(person_db, retval,
                "error while closing cursor");
    }
    retval = txn->commit(txn, 0);
    if(retval != 0)
    {
        env->err(env, retval,
                "error while committing transaction");
        return -1;
    }
    return 0;
}

void close_env()
{
    int retval = 0;
    if(person_by_name_db != NULL)
    {
        retval = person_by_name_db->close(person_by_name_db, 0);
        if(retval != 0)
            printf("error closing person_by_name_db\n");
    }
    if(person_db != NULL)
    {
        retval = person_db->close(person_db, 0);
        if(retval != 0)
            printf("error closing person_db\n");
    }
    if(env != NULL)
    {
        retval = env->close(env, 0);
        if(retval != 0)
            printf("error closing env\n");
    }
}

int main(int argc, char **argv)
{
    if(open_env())
        return;
    if(open_db())
        return;
    if(load_db())
        return;
    dump_db();
    read_rec();
    delete_rec();
    printf("dump after delete\n");
    dump_db();
    update_rec();
    printf("dump after update\n");
    dump_db();
    close_env();
}
