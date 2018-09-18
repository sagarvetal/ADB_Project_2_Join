package com.join.utility;

import java.nio.ByteBuffer;

public class SortService {

	final int tupleSize;
	
	public SortService(final int tupleSize) {
		this.tupleSize = tupleSize;
	}
	
	public void quickSort(final int left, final int right, final ByteBuffer outputBuffer) {
		if( left < right) {
			final int r = partition(left, right, outputBuffer) ;
			quickSort( left, r - tupleSize, outputBuffer) ;
			quickSort( r + tupleSize, right, outputBuffer) ;
		}
	}

	private int partition(final int left, final int right, final ByteBuffer outputBuffer) {
		final int pivot = right;
		int i = left - tupleSize;
		for (int j = left; j < right; j = j + tupleSize) {
			for (int k = 0; k != SizeConstants.KEY_SIZE; ++k) {
				if (outputBuffer.get(j+k) < outputBuffer.get(pivot + k)) {
					i = i + tupleSize;
					exchange(i, j, outputBuffer);
					break;
				} else if (outputBuffer.get(j + k) > outputBuffer.get(pivot + k)) {
					break;
				}
			}
		}
		i = i + tupleSize;
		exchange(i, right, outputBuffer);
		return i;
	}
	
	private void exchange(final int i, final int j, final ByteBuffer outputBuffer) {
		if (i == j) {
			return;
		}
		
		final int currentPosition = outputBuffer.position() ;
		
		outputBuffer.position(i) ;
		final byte[] tempI = new byte[tupleSize] ;
		outputBuffer.get(tempI) ;
		
		outputBuffer.position(j) ;
		final byte[] tempJ = new byte[tupleSize] ;
		outputBuffer.get(tempJ) ;
		
		outputBuffer.position(j) ;
		outputBuffer.put(tempI) ;
		
		outputBuffer.position(i) ;
		outputBuffer.put(tempJ) ;
		
		outputBuffer.position(currentPosition) ;
	}
	
}
