package com.join.utility;

public class SizeConstants {

	public static final int KEY_SIZE = 8;
	public static final int T1_TUPLE_SIZE = 101;
	public static final int T2_TUPLE_SIZE = 28;
	public static final int JOIN_TUPLE_SIZE = T1_TUPLE_SIZE + T2_TUPLE_SIZE - KEY_SIZE - 1;
	public static final int BLOCK_SIZE = 4096;
	public static final int T1_BLOCK_SIZE = (BLOCK_SIZE / T1_TUPLE_SIZE) * T1_TUPLE_SIZE;
	public static final int T2_BLOCK_SIZE = ((int)(BLOCK_SIZE * 0.89) / T2_TUPLE_SIZE) * T2_TUPLE_SIZE;;
	public static final int MAIN_MEMORY_SIZE = CommonUtility.getMemorySizeInMB() == 5 ? (int)(5 * 0.5 * 1024 * 1024) : (int)(10 * 0.5 * 1024 * 1024);
}
