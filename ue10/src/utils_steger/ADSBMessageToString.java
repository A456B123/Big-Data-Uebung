package utils_steger;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class ADSBMessageToString implements MapFunction<Row, String> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String map(Row arg0) throws Exception {
        return arg0.toString() + "\n";
	}
	
}
