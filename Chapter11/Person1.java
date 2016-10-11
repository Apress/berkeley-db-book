package chap_11;

public class Person1 
{
    private Long ssn_;
    private String name_;
    private String dob_;

    public Person1(Long ssn, 
            String name, 
            String dob)
    {
        ssn_ = ssn;
        name_ = name;
        dob_ = dob;
    }

    public Long getSSN()
    {
        return ssn_;
    }

    public String getName()
    {
        return name_;
    }

    public String getDOB()
    {
        return dob_;
    }

    public String toString()
    {
        return "PersonEnt: ssn = " + ssn_ + " name = " + name_ 
            + " dob = " + dob_;
    }
}
