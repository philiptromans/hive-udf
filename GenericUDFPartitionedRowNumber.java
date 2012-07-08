import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;

/**
 * This UDF allows you to replicate the functionality of MS SQL Server's ROW_NUMBER() OVER (PARTITION BY ... ORDER BY...) function.
 * Typical usage is along the lines of:
 * 
 * 	SELECT *, partitionedRowNumber(a,b) as rowindex FROM (
 * 		SELECT *
 * 			FROM table
 * 			DISTRIBUTE BY a,b
 * 			SORT BY a,b
 * 		) i2;
 */
@Description(name = "partitionedRowNumber", value = "_FUNC_(a, [...]) - Assumes that incoming data is SORTed and DISTRIBUTEd according to the given columns, and then returns the row number for each row within the partition,")
public class GenericUDFPartitionedRowNumber extends GenericPartitioningUDF {
	private LongWritable rowIndex = new LongWritable(0);
	private Object[] partitionColumnValues;
	private ObjectInspector[] objectInspectors;
	private int[] sortDirections; // holds +1 (for compare() > 0), 0 for unknown, -1 (for compare() < 0)

	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length == 0) {
			throw new UDFArgumentLengthException(
					"The function partitionedRowNumber expects at least 1 argument.");
		}

		partitionColumnValues = new Object[arguments.length];

		for (ObjectInspector oi : arguments) {
			if (ObjectInspectorUtils.isConstantObjectInspector(oi)) {
				throw new UDFArgumentException("No constant arguments should be passed to partitionedRowNumber.");
			}
		}
		
		objectInspectors = arguments;

		sortDirections = new int[arguments.length];

		return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] args) throws HiveException {
		assert (args.length == partitionColumnValues.length);

		for (int i = 0; i < args.length; i++) {
			if (partitionColumnValues[i] == null) {
				partitionColumnValues[i] = ObjectInspectorUtils.copyToStandardObject(args[i].get(),
						objectInspectors[i]);
			} else if (!nullSafeEquals(args[i].get(), partitionColumnValues[i])) {
				// check sort directions. We know the elements aren't equal.
				int newDirection = collapseToIndicator(ObjectInspectorUtils.compare(args[i].get(),
						objectInspectors[i], partitionColumnValues[i], objectInspectors[i]));
				if (sortDirections[i] == 0) { // We don't already know what the
												// sort direction should be
					sortDirections[i] = newDirection;
				} else if (sortDirections[i] != newDirection) {
					throw new HiveException("Data in column: " + i
							+ " does not appear to be consistently sorted, so partitionedRowNumber cannot be used.");
				}

				// reset everything (well, the remaining column values, because the
				// previous ones haven't changed.
				for (int j = i; j < args.length; j++) {
					partitionColumnValues[j] = ObjectInspectorUtils.copyToStandardObject(
							args[j].get(), objectInspectors[j]);
				}
				rowIndex.set(1);
				return rowIndex;
			}
		}

		// partition columns are identical. Increment and continue.
		rowIndex.set(rowIndex.get() + 1);
		return rowIndex;
	}

	@Override
	public String getDisplayString(String[] children) {
		return "partitionedRowNumber(" + StringUtils.join(children, ", ") + ")";
	}
}

