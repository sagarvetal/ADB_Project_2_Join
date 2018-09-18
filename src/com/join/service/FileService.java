package com.join.service;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileService {
	
	private int readCount;
	private int writeCount;
	private int blockSize;
	
	public FileService(final int blockSize) {
		this.blockSize = blockSize;
	}

	public FileService() {
	}
	
	public void readInputFile(final FileChannel fileChannel, final ByteBuffer inputBuffer) {
		inputBuffer.clear();
		try {
			final int bytesRead = fileChannel.read(inputBuffer);
			readCount = readCount + bytesRead / blockSize;
			if(bytesRead % blockSize != 0) {
				readCount++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		inputBuffer.flip();
	}
	
	public int getReadCount() {
		return readCount;
	}
	
	public void writeOutputFile(final FileChannel fileChannel, final ByteBuffer outputBuffer) {
		try {
			final int byteWrites = fileChannel.write(outputBuffer) ;
			writeCount = writeCount + byteWrites / blockSize ;
			if(byteWrites % blockSize != 0) {
				writeCount++;
			}
		} catch ( IOException e ) {
			e.printStackTrace();
		}
		outputBuffer.clear() ;
	}

	public int getWriteCount() {
		return writeCount;
	}

	public void deleteOldFiles(final String filePath) {
		final File oldFiles = new File(filePath);
		for(final File oldFile : oldFiles.listFiles()) {
			if(!oldFile.isDirectory()) {
				oldFile.delete();
			}
		}
	}
}
