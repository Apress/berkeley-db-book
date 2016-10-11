package chap_11;

import java.io.*;
import java.util.*;
import com.sleepycat.db.*;
import com.sleepycat.collections.*;
import com.sleepycat.bind.serial.*;
import com.sleepycat.bind.tuple.*;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.EntityBinding;

public class JavaEnv
{

    public static final int SerialBinding = 1;
    public static final int TupleBinding = 2;
    public static final int EntityBinding = 3;
    public static final int EntityTupleSerialBinding = 4;

    private Person manish 
        = new Person(new Long(111223333), "Manish Pandey", "jan 13 1971");
    private Person shirish 
        = new Person(new Long(111223334), "Shirish Rai", "dec 23 1974");
    private Person roland 
        = new Person(new Long(111443335), "Roland Hendel", "feb 13 1973");

    private Person1 manish1 
        = new Person1(new Long(111223333), "Manish Pandey", "jan 13 1971");
    private Person1 shirish1 
        = new Person1(new Long(111223334), "Shirish Rai", "dec 23 1974");
    private Person1 roland1 
        = new Person1(new Long(111443335), "Roland Hendel", "feb 13 1973");

    private EnvironmentConfig envConfig_ = null;
    private Environment env_ = null;
    private File envHome_ = new File("./chap11_env");

    private DatabaseConfig dbConfig_ = null;
    private Database personDb_ = null;
    private Database classCatalogDb_ = null;
    private StoredClassCatalog classCatalog_ = null;

    private StoredMap personMap_ = null;
    private StoredValueSet personSet_ = null;

    Environment getEnv()
    {
        return env_;
    }

    private static class PersonBinding extends SerialSerialBinding
    {
        private PersonBinding(ClassCatalog catalog,
                Class keyClass, Class dataClass)
        {
            super(catalog, keyClass, dataClass);
        }

        public Object entryToObject(Object keyIn,
                Object dataIn)
        {
            PersonKey key = (PersonKey)keyIn;
            PersonData data = (PersonData) dataIn;
            return new Person(key.getSSN(), data.getName(),
                    data.getDOB());
        }

        public Object objectToKey(Object o)
        {
            Person1 p = (Person1) o;
            return new PersonKey(p.getSSN());
        }

        public Object objectToData(Object o)
        {
            Person1 p = (Person1) o;
            return new PersonData(p.getName(), p.getDOB());
        }
    }

    private static class PersonKeyBinding extends TupleBinding
    {
        private PersonKeyBinding()
        {}

        public Object entryToObject(TupleInput input)
        {
            Long ssn = input.readLong();
            return new PersonKey(ssn);
        }

        public void objectToEntry(Object o, TupleOutput output)
        {
            PersonKey key = (PersonKey)o;
            output.writeLong(key.getSSN());
        }
    }

    private static class PersonTupleSerialBinding extends TupleSerialBinding
    {
        private PersonTupleSerialBinding(ClassCatalog catalog, Class dataClass)
        {
            super(catalog, dataClass);
        }

        public Object entryToObject(TupleInput keyIn, Object dataIn)
        {
            Long ssn = keyIn.readLong();
            Person p = (Person) dataIn;
            p.setSSN(ssn);
            return p;
        }

        public void objectToKey(Object o, TupleOutput to)
        {
            Person p = (Person) o;
            to.writeLong(p.getSSN());
        }

        public Object objectToData(Object o)
        {
            return o;
        }
    }

    private void createEnv() throws Exception
    {
        try
        {

            envConfig_ = new EnvironmentConfig();
            envConfig_.setErrorStream(System.err);
            envConfig_.setErrorPrefix("chap_11");
            envConfig_.setAllowCreate(true);
            envConfig_.setInitializeCache(true);
            envConfig_.setTransactional(true);
            envConfig_.setInitializeLocking(true);

            env_ = new Environment(envHome_, envConfig_);
        }
        catch(Exception e)
        {
            System.err.println("createEnv: " + e.toString());
            throw e;
        }
    }

    private void createDbHandle() throws Exception
    {
        try
        {
            dbConfig_ = new DatabaseConfig();
            dbConfig_.setErrorStream(System.err);
            dbConfig_.setType(DatabaseType.BTREE);
            dbConfig_.setAllowCreate(true);
            dbConfig_.setTransactional(true);

            dbConfig_.setErrorPrefix("chap_11:PersonDb");
            personDb_ = env_.openDatabase(null, "Person_Db", null, dbConfig_);

            classCatalogDb_ = 
                env_.openDatabase(null, "ClassCatalog_Db", null, dbConfig_);
            classCatalog_ = new StoredClassCatalog(classCatalogDb_);
        }
        catch(Exception e)
        {
            System.err.println("createDbHandle: " + e.toString());
            throw e;
        }

    }

    private void addToPersonMap()
    {
        try
        {
            SerialBinding personKeyBinding =
                new SerialBinding(classCatalog_, PersonKey.class);

            EntityBinding personDataBinding = 
                new PersonBinding(classCatalog_,
                        PersonKey.class, PersonData.class);

            personMap_ = 
                new StoredMap(personDb_, personKeyBinding,
                        personDataBinding, true);

            personSet_ = (StoredValueSet)personMap_.values();

            personSet_.add(manish1);
            personSet_.add(shirish1);
            personSet_.add(roland1);
        }
        catch(Exception e)
        {
            System.err.println("addToPersonMap: " + e.toString());
        }
    }

    private void addToPersonMap1()
    {
        try
        {
            PersonKeyBinding keyBinding = new PersonKeyBinding();

            PersonTupleSerialBinding personTupleSerialBinding =
                new PersonTupleSerialBinding(classCatalog_, Person.class);

            personMap_ =
                new StoredMap(personDb_, keyBinding, 
                        personTupleSerialBinding, true);

            personSet_ = 
                (StoredValueSet)personMap_.values();

            personSet_.add(manish);
            personSet_.add(shirish);
            personSet_.add(roland);
        }
        catch(Exception e)
        {
            System.err.println("addToPersonMap1: " + e.toString());
        }
    }

    void loadEntries(int bindingType) throws Exception
    {
        EntryBinding binding = null; 
        switch(bindingType)
        {
            case SerialBinding:
                binding = 
                    new SerialBinding(classCatalog_, Person.class);
                addPersonEntries(binding);
                break;
            case TupleBinding:
                binding = new PersonTupleBinding();
                addPersonEntries(binding);
                break;
            case EntityBinding:
                addToPersonMap();
                break;
            case EntityTupleSerialBinding:
                addToPersonMap1();
                break;
            default:
                System.out.println("Unknown binding");
                break;
        }
    }

    private void addPersonEntries(EntryBinding binding) throws Exception
    {
        Transaction txn = null;
        try
        {
            txn = env_.beginTransaction(null, null);

            Person p = null;
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();

            LongBinding.longToEntry(manish.getSSN(), key);
            binding.objectToEntry(manish, data);
            personDb_.put(null, key, data);

            LongBinding.longToEntry(shirish.getSSN(), key);
            binding.objectToEntry(shirish, data);
            personDb_.put(null, key, data);

            LongBinding.longToEntry(roland.getSSN(), key);
            binding.objectToEntry(roland, data);
            personDb_.put(null, key, data);
            txn.commit();
        }
        catch (DatabaseException e)
        {
            txn.abort();
            System.err.println("addPersonEntries: " + e.toString());
            throw e;
        }
    }

    void dumpEntries(int bindingType) throws Exception
    {
        
        EntryBinding binding = null;
        switch(bindingType)
        {
            case SerialBinding:
                binding = 
                    new SerialBinding(classCatalog_,
                            Person.class);
                cursorPrint(binding);
                break;
            case TupleBinding:
                binding = new PersonTupleBinding();
                cursorPrint(binding);
                break;
            case EntityBinding:
            case EntityTupleSerialBinding:
                iteratorPrint();
                return;
            default:
                System.out.println("Unknown binding " + binding);
                return;
        }
        
    }

    private void cursorPrint(EntryBinding binding) throws Exception
    {
        Transaction txn = null;
        Cursor cursor = null;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        try
        {
            txn = env_.beginTransaction(null, null);
            Person p = new Person();

            cursor = personDb_.openCursor(txn, null);
            while(cursor.getNext(key, data, LockMode.DEFAULT)
                    == OperationStatus.SUCCESS)
            {
                p = (Person)(binding.entryToObject(data));
                System.out.println("dumpEntries: " + p);
            }
            cursor.close();
            cursor = null;
            txn.commit();
        }
        catch (DatabaseException e)
        {
            if(cursor != null)
                cursor.close();
            txn.abort();
            System.err.println("addPersonEntries: " + e.toString());
            throw e;
        }
    }

    private void iteratorPrint() throws Exception
    {
        Iterator i = personSet_.iterator();
        try
        {
            while(i.hasNext())
            {
                System.out.println(i.next().toString());
            }
        }
        finally
        {
            StoredIterator.close(i);
        }
    }

    public JavaEnv()
    {
        try
        {
            createEnv();
            createDbHandle();
            //loadEntries(binding);
            //dumpEntries(binding);
        }
        catch(DatabaseException e)
        {
            System.err.println("LoadDbs: " + e.toString());
        }
        catch(Exception e)
        {
            System.err.println("LoadDbs: non DB exception caught " + 
                    e.toString());
        }
    }
}
