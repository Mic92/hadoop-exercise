package examples.dns;

/**
 * The class defines constants and functions relating the crawling methods such
 * as AXFR, NSEC walking and miniPseudoTransfer.
 */
public final class Method
{
    public static final int AXFR = 0;
    public static final int NSEC = 1;
    public static final int OTHER = 2;

    /**
     * Converts a numeric method into a String representation.
     * @return the canonical string representation of the method.
     */
    public static String string(int i)
    {
        return i == AXFR ? "AXFR" : (i == NSEC ? "NSEC" : "Other");
    }

    /**
     * Converts a String representation of a method into its corresponding
     * numeric value.
     * @return the method code
     */
    public static int value(String s)
    {
        return s.equals("AXFR") ? AXFR : (s.equals("NSEC") ? NSEC : OTHER);
    }
}
