package chap_11;

import com.sleepycat.bind.tuple.*;

public class PersonTupleBinding extends TupleBinding
{
    public void objectToEntry(Object o, TupleOutput out)
    {
        Person p = (Person)o;
        out.writeLong(p.getSSN());
        out.writeString(p.getName());
        out.writeString(p.getDOB());
    }

    public Object entryToObject(TupleInput in)
    {
        Long ssn = in.readLong();
        String name = in.readString();
        String dob = in.readString();

        Person p = new Person(ssn, name, dob);
        return p;
    }
}
