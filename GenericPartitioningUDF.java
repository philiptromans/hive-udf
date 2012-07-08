import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

public abstract class GenericPartitioningUDF extends GenericUDF {

	/**
	 * Takes the output of compare() and scales it to either, +1, 0 or -1.
	 * 
	 * @param val
	 * @return
	 */
	protected static int collapseToIndicator(int val) {
		if (val > 0) {
			return 1;
		} else if (val == 0) {
			return 0;
		} else {
			return -1;
		}
	}

	/**
	 * Wraps Object.equals, but allows one or both arguments to be null. Note that nullSafeEquals(null, null) == true.
	 * @param o1 First object
	 * @param o2 Second object
	 * @return
	 */
	protected static boolean nullSafeEquals(Object o1, Object o2) {
		if (o1 == null && o2 == null) {
			return true;
		} else if (o1 == null || o2 == null) {
			return false;
		} else {
			return (o1.equals(o2));
		}
	}

}

