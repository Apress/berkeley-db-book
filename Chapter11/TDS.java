
package chap_11;

import com.sleepycat.collections.*;
import com.sleepycat.db.*;

public class TDS
{
    static JavaEnv javaEnv_ = null;
    static int bindingType_ = 0;

    public TDS()
    {
        javaEnv_ = new JavaEnv();
    }

    private void startTDS(int bindingType)
    {
        bindingType_ = bindingType;
        try
        {
            TransactionRunner runner = 
                new TransactionRunner(javaEnv_.getEnv());
            runner.run(new LoadDbs());
            runner.run(new DumpDbs());
        }
        catch(Exception e)
        {
            System.err.println("Exception caught while running transaction");
            e.printStackTrace();
        }
    }

    private class LoadDbs implements TransactionWorker
    {
        public void doWork() throws Exception
        {
            javaEnv_.loadEntries(bindingType_);
        }
    }

    private class DumpDbs implements TransactionWorker
    {
        public void doWork() throws Exception
        {
            javaEnv_.dumpEntries(bindingType_);
        }
    }

    public static void main(String args[])
    {
        int bindingType = Integer.parseInt(args[0]);
        TDS tds = new TDS();
        tds.startTDS(bindingType);
    }
}
