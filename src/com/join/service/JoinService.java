package com.join.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import com.join.buffers.Buffer;
import com.join.utility.CommonUtility;
import com.join.utility.FileConstants;
import com.join.utility.SizeConstants;
import com.join.utility.SortService;

public class JoinService {
	
	int tupleSize;
	int blockSize;
	int mainMemorySize;
	StringBuilder sublistFilePath;
	int totalBlocks;
	int totalFills;
	int mainMemoryBlocks;
	File file;
	int fileSize;
	StringBuilder fileName;
	ByteBuffer outputBuffer ;
	Buffer[] sublistBufferArray ;
	Buffer[] relationBufferArray ;
	int sortReadCount;
	int sortWriteCount;
	int mergeReadCount;
	int mergeWriteCount;
	int joinReadCount;
	int joinWriteCount;
	int nonEmptyBufferListSize ;
	
	public JoinService(final String filePath, final int tupleSize, final int blockSize) {
		this.tupleSize = tupleSize;
		this.blockSize = blockSize;
		mainMemorySize = (SizeConstants.MAIN_MEMORY_SIZE / blockSize) * blockSize;
		sublistFilePath = new StringBuilder(FileConstants.SUBLISTS_FILE_PATH);
		sublistFilePath.append(FileConstants.SUBLIST_FILE_NAME);
		file = new File(filePath);
		fileName = new StringBuilder(file.getName().substring(0, file.getName().lastIndexOf(".")));
		fileSize = (int)file.length();
		System.out.println("Total Number of Tuples : " + fileSize / tupleSize);
		totalBlocks = CommonUtility.getTotalBlocks(fileSize, blockSize);
		System.out.println("Total Number of Blocks : " + totalBlocks);
		totalFills = CommonUtility.getTotalFills(fileSize, mainMemorySize);
		//System.out.println("Total Main Memory Fills : " + totalFills);
		mainMemoryBlocks = CommonUtility.getMainMemoryBlocks(mainMemorySize, blockSize);
		//System.out.println("Total Main Memory Blocks : " + mainMemoryBlocks);
		//System.out.println("-------------------------------------");
	}

	public JoinService() {
	}

	public int sort() {
		
		try {
			final FileChannel inputFileChannel = FileChannel.open(Paths.get(file.getPath()), EnumSet.of(StandardOpenOption.READ));
			final ByteBuffer inputBuffer = ByteBuffer.allocate(mainMemorySize);
			final FileService fileService =  new FileService(blockSize);
			final SortService sortService =  new SortService(tupleSize);
			
			for(int run = 0; run < totalFills-1; run++) {
				fileService.readInputFile(inputFileChannel, inputBuffer);
				sortService.quickSort(0, mainMemorySize - tupleSize, inputBuffer);
				write(inputBuffer, fileService, run) ;
			}
			
			if (fileSize % mainMemorySize != 0) {
				fileService.readInputFile(inputFileChannel, inputBuffer);
				sortService.quickSort(0, ((fileSize % mainMemorySize) / tupleSize * tupleSize) - tupleSize, inputBuffer);
				write(inputBuffer, fileService, totalFills - 1);
			} else {
				fileService.readInputFile(inputFileChannel, inputBuffer);
				sortService.quickSort(0, mainMemorySize - tupleSize, inputBuffer);
				write(inputBuffer, fileService, totalFills - 1);
			}

			if (inputFileChannel.isOpen()) {
				try {
					inputFileChannel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			sortReadCount = fileService.getReadCount();
			sortWriteCount = fileService.getWriteCount();
		} catch (FileNotFoundException fnfException) {
			System.err.println("File not exits: " + fnfException.getMessage());
			return 0;
		} catch (IOException ioException) {
			System.err.println("Erro while reading: " + ioException.getMessage());
			return 0;
		}

		return sortReadCount + sortWriteCount;
	}
	
	private void write(final ByteBuffer inputBuffer, final FileService fileService, final int run) throws FileNotFoundException, IOException {
		final String sublistFileName = sublistFilePath.toString() + fileName + "_" + run + ".txt";
		final FileChannel outputFileChannel = FileChannel.open(Files.createFile(Paths.get(sublistFileName)), EnumSet.of(StandardOpenOption.WRITE));
		fileService.writeOutputFile(outputFileChannel, inputBuffer);
		if (outputFileChannel.isOpen()) {
			try {
				outputFileChannel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public int merge() {
		try {
			FileChannel outputChannel = FileChannel.open(Files.createFile(Paths.get(FileConstants.OUTPUT_FILE_PATH + fileName + "_sorted.txt")),
																EnumSet.of(StandardOpenOption.APPEND));
			
			fillSublistBuffers();

			final int outputBufferTuples = outputBuffer.capacity() / tupleSize;
			int outputBufferRefills = CommonUtility.getTotalNoOfTuples(fileSize, tupleSize) / outputBufferTuples;

			arrangeSublistBuffers();

			while (outputBufferRefills != 0) {
				int i = outputBufferTuples;
				while (i != 0) {
					outputBuffer.put(getSmallestTuple());
					--i;
				}
				
				final int bytesWritten = writeOutputBuffer(outputChannel);
				mergeWriteCount += bytesWritten / blockSize;
				if(bytesWritten % blockSize != 0) {
					mergeWriteCount++;
				}
				--outputBufferRefills;
			}

			int j = CommonUtility.getTotalNoOfTuples(fileSize, tupleSize) % outputBufferTuples;
			while (j != 0) {
				outputBuffer.put(getSmallestTuple());
				--j;
			}
			final int bytesWritten = writeOutputBuffer(outputChannel);
			mergeWriteCount += bytesWritten / blockSize;
			if(bytesWritten % blockSize != 0) {
				mergeWriteCount++;
			}

			if (outputChannel.isOpen()) {
				try {
					outputChannel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			for (int i = 0; i != sublistBufferArray.length; i++) {
				sublistBufferArray[i].setBuffer(null);
				if (sublistBufferArray[i].getFileChannel().isOpen()) {
					try {
						sublistBufferArray[i].getFileChannel().close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			sublistBufferArray = null;
			outputBuffer = null;
			
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
		return mergeReadCount + mergeWriteCount;
	}
	
	private void fillSublistBuffers() {
		try {
			// Calculate SublistBuffer Size and Output buffer size
			final int sublistBufferSize = (int) ((mainMemoryBlocks * 0.7) / totalFills) * blockSize;
			final int outputBufferSize = (int) ((mainMemoryBlocks * 0.3) + ((mainMemoryBlocks * 0.7) % totalFills)) * blockSize;

			sublistBufferArray = new Buffer[totalFills];

			// Create sublist buffers and perform first read.
			for (int i = 0; i < totalFills-1; i++) {
				final String sublistFileName = sublistFilePath.toString() + fileName + "_" + i + ".txt";
				final FileChannel fileChannel = FileChannel.open(Paths.get(sublistFileName), EnumSet.of(StandardOpenOption.READ));
				sublistBufferArray[i] = new Buffer(ByteBuffer.allocate(sublistBufferSize), 0, mainMemorySize - 1, fileChannel);
				sublistBufferArray[i].readBuffer();
				final int bytesRead = sublistBufferArray[i].getBuffer().limit() - sublistBufferArray[i].getBuffer().position();
				mergeReadCount += bytesRead / blockSize;
				if(bytesRead % blockSize != 0) {
					mergeReadCount++;
				}
			}

			// Create last sublist buffer. (It can be smaller than others)
			final String sublistFileName = sublistFilePath.toString() + fileName + "_" + (totalFills - 1) + ".txt";
			final FileChannel fileChannel = FileChannel.open(Paths.get(sublistFileName), EnumSet.of(StandardOpenOption.READ));
			sublistBufferArray[totalFills - 1] = new Buffer(ByteBuffer.allocate(sublistBufferSize), 0, new File(sublistFileName).length() - 1, fileChannel);
			sublistBufferArray[totalFills - 1].readBuffer();
			final int bytesRead = sublistBufferArray[totalFills - 1].getBuffer().limit()	- sublistBufferArray[totalFills - 1].getBuffer().position();
			mergeReadCount += bytesRead / blockSize;
			if(bytesRead % blockSize != 0) {
				mergeReadCount++;
			}
			
			// Create output buffer
			outputBuffer = ByteBuffer.allocate(outputBufferSize);

		} catch (IOException e) {
			System.out.println("The following error occurred during initial sublist buffer load: " + e.getMessage());
		}
	}
	
	private void arrangeSublistBuffers() {
		nonEmptyBufferListSize = sublistBufferArray.length - 1;
		for (int i = (sublistBufferArray.length / 2) - 1; i != -1; --i) {
			findSmallestSublistBuffer(i);
		}
	}

	private void findSmallestSublistBuffer(int i) {
		final int leftChild = (2 * i) + 1;
		final int rightChild = ((2 * i) + 1) + 1;
		int smallest;
		if (leftChild <= nonEmptyBufferListSize && sublistBufferArray[leftChild].compareTo(sublistBufferArray[i], SizeConstants.KEY_SIZE) < 0) {
			smallest = leftChild;
		} else {
			smallest = i;
		}
		if (rightChild <= nonEmptyBufferListSize && sublistBufferArray[rightChild].compareTo(sublistBufferArray[smallest], SizeConstants.KEY_SIZE) < 0) {
			smallest = rightChild;
		}
		if (smallest != i) {
			final Buffer tempBuffer = sublistBufferArray[smallest];
			sublistBufferArray[smallest] = sublistBufferArray[i];
			sublistBufferArray[i] = tempBuffer;
			findSmallestSublistBuffer(smallest);
		}
	}

	private byte[] getSmallestTuple() {
		final byte[] tuple = new byte[tupleSize];
		try {
			sublistBufferArray[0].getBuffer().get(tuple);
		} catch (BufferUnderflowException e) {
		}

		if (sublistBufferArray[0].getBuffer().position() == sublistBufferArray[0].getBuffer().limit()) {
			if (!sublistBufferArray[0].readBuffer()) {
				final Buffer tempBuffer = sublistBufferArray[0];
				sublistBufferArray[0] = sublistBufferArray[nonEmptyBufferListSize];
				sublistBufferArray[nonEmptyBufferListSize] = tempBuffer;
				--nonEmptyBufferListSize;
			} else {
				final int bytesRead = sublistBufferArray[0].getBuffer().limit() - sublistBufferArray[0].getBuffer().position();
				mergeReadCount += bytesRead / blockSize;
				if(bytesRead % blockSize != 0) {
					mergeReadCount++;
				}
			}
		}

		findSmallestSublistBuffer(0);
		return tuple;
	}
	
	private int writeOutputBuffer(final FileChannel outputChannel) {
		try {
			outputBuffer.flip();
			final int bytesWritten = outputChannel.write(outputBuffer);
			if (outputBuffer.position() != outputBuffer.limit()) {
				throw new IOException("Output Buffer write not successfull");
			}
			outputBuffer.clear();
			return bytesWritten;
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	public Hashtable<String, String> createHashTable(final BufferedReader br, final int t1BufferSize, final int tupleSize) throws IOException {
		final Hashtable<String, String> htRelation = new Hashtable<>();
		int noOfTuples = t1BufferSize / tupleSize;
		
		while(noOfTuples != 0) {
			final String tuple = br.readLine();
			if(tuple != null) {
				htRelation.put(tuple.substring(0, SizeConstants.KEY_SIZE), tuple);
			}else {
				break;
			}
			--noOfTuples;
		}
		return htRelation;
	}
	
	public int nestedLoopJoin() {
		joinReadCount = 0;
		joinWriteCount = 0;
		try {
			final FileChannel outputChannel = FileChannel.open(Files.createFile(Paths.get(FileConstants.OUTPUT_FILE_PATH + FileConstants.NESTED_LOOP_JOIN_FILE_NAME)),
					EnumSet.of(StandardOpenOption.APPEND));
			final File t1 = new File(FileConstants.INPUT_FILE_PATH + FileConstants.RELATION_T1);
			final File t2 = new File(FileConstants.INPUT_FILE_PATH + FileConstants.RELATION_T2);
			
			//Calculate tuple size for join result
			tupleSize = SizeConstants.JOIN_TUPLE_SIZE;
			
			//Calculate block size for join result
			blockSize = (SizeConstants.BLOCK_SIZE / tupleSize) * tupleSize;
			
			// Calculate T1 & T2 Buffer Size and Output buffer size
			final int t1BufferSize =  ((int)((SizeConstants.MAIN_MEMORY_SIZE * 0.2) / SizeConstants.T1_BLOCK_SIZE) * SizeConstants.T1_BLOCK_SIZE);
			final int t2BufferSize = ((int)((SizeConstants.MAIN_MEMORY_SIZE * 0.1) / SizeConstants.T2_BLOCK_SIZE) * SizeConstants.T2_BLOCK_SIZE);
			final int outputBufferSize = ((int)((SizeConstants.MAIN_MEMORY_SIZE * 0.1) / blockSize) * blockSize);
			
			final BufferedReader br = new BufferedReader(new FileReader(t1));
			int t1Fills = (int) (t1.length() / t1BufferSize);
			if(t1.length() % t1BufferSize != 0) {
				t1Fills++;
			}

			final Buffer t2Buffer = createAndFillRelationBuffer(t2, t2BufferSize, SizeConstants.T2_BLOCK_SIZE);
			
			// Create output buffer
			outputBuffer = ByteBuffer.allocate(outputBufferSize);
			
			int i = 0;
			final int outputBufferTuples = outputBuffer.capacity() / tupleSize;
			
			final BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File (FileConstants.OUTPUT_FILE_PATH + FileConstants.NLJ_STUDENT_GPA_FILE_NAME)));
			final FileReader reader = new FileReader(new File(FileConstants.GRADE_PROPERTY_FILE_PATH));  
		    final Properties p = new Properties();  
		    p.load(reader); 
			
			while (t1Fills != 0) {
				
				final Hashtable<String, String> htRelation = createHashTable(br, t1BufferSize, SizeConstants.T1_TUPLE_SIZE);
				joinReadCount += (htRelation.size() / (SizeConstants.T1_BLOCK_SIZE / SizeConstants.T1_TUPLE_SIZE));
				if(htRelation.size() % (SizeConstants.T1_BLOCK_SIZE / SizeConstants.T1_TUPLE_SIZE) != 0) {
					joinReadCount++;
				}
				LinkedHashMap<String, String> hmGPA = new LinkedHashMap<>();
				
				while (refillRelationBuffer(t2Buffer, SizeConstants.T2_BLOCK_SIZE)) {
					
					final byte[] tuple = new byte[SizeConstants.T2_TUPLE_SIZE];
					t2Buffer.getBuffer().get(tuple, 0, SizeConstants.T2_TUPLE_SIZE);
					final String r2tuple = new String(tuple); 
					
					final String r1Tuple = htRelation.get(r2tuple.substring(0, SizeConstants.KEY_SIZE));
					if (r1Tuple != null) {
						final StringBuilder joinTuple = new StringBuilder(r1Tuple);
						joinTuple.append(r2tuple.substring(SizeConstants.KEY_SIZE, SizeConstants.T2_TUPLE_SIZE));
						outputBuffer.put(joinTuple.toString().getBytes());
						i++;
						
						final double gradePoints = Double.parseDouble(p.getProperty(joinTuple.substring(tupleSize-5, tupleSize-1).trim()));
						final double credits = Double.parseDouble(joinTuple.substring(tupleSize-7, tupleSize-5).trim());
						final String gpa = hmGPA.get(joinTuple.substring(0, SizeConstants.KEY_SIZE));
						
						if(gpa != null) {
							final double gradeSum = Double.parseDouble(gpa.split(",")[0]) + (gradePoints * credits);
							final double creditSum = Double.parseDouble(gpa.split(",")[1]) + credits;
							hmGPA.put(joinTuple.substring(0, SizeConstants.KEY_SIZE), gradeSum+","+creditSum);
						} else {
							final double gradeSum = (gradePoints * credits);
							final double creditSum = credits;
							hmGPA.put(joinTuple.substring(0, SizeConstants.KEY_SIZE), gradeSum+","+creditSum);
						}
						
						if(i == outputBufferTuples) {
							final int bytesWritten = writeOutputBuffer(outputChannel);
							joinWriteCount += bytesWritten / blockSize;
							if(bytesWritten % blockSize != 0) {
								joinWriteCount++;
							}
							i=0;
						}
					}
				}
				t2Buffer.setCurrentPosition(0);
				
				for(Map.Entry<String, String> entry : hmGPA.entrySet()) {
					calculateAndWriteGPA(bufferedWriter, entry.getKey(), Double.parseDouble(entry.getValue().split(",")[0]), Double.parseDouble(entry.getValue().split(",")[1]));
				}
				hmGPA = null;
				
				t1Fills--;
			}
			
			if(i != 0) {
				final int bytesWritten = writeOutputBuffer(outputChannel);
				joinWriteCount += bytesWritten / blockSize;
				if(bytesWritten % blockSize != 0) {
					joinWriteCount++;
				}
			}
			
			bufferedWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return joinReadCount + joinWriteCount;
	}
	
	public int sortBasedJoin(final JoinService t1, final JoinService t2) {
		joinReadCount = 0;
		joinWriteCount = 0;
		try {
			final FileChannel outputChannel = FileChannel.open(Files.createFile(Paths.get(FileConstants.OUTPUT_FILE_PATH + FileConstants.SORT_BASED_JOIN_FILE_NAME)),
																EnumSet.of(StandardOpenOption.APPEND));
			final File sortedT1 = new File(FileConstants.OUTPUT_FILE_PATH + FileConstants.RELATION_T1.substring(0, FileConstants.RELATION_T1.lastIndexOf(".")) + "_sorted.txt");
			final File sortedT2 = new File(FileConstants.OUTPUT_FILE_PATH + FileConstants.RELATION_T2.substring(0, FileConstants.RELATION_T2.lastIndexOf(".")) + "_sorted.txt");
			
			//Calculate tuple size for join result
			tupleSize = SizeConstants.JOIN_TUPLE_SIZE;
			
			//Calculate block size for join result
			blockSize = (SizeConstants.BLOCK_SIZE / tupleSize) * tupleSize;
			
			// Calculate T1 & T2 Buffer Size and Output buffer size
			final int t1BufferSize = ((int)(t1.mainMemorySize * 0.3) / t1.blockSize) * t1.blockSize;
			final int t2BufferSize = ((int)(t2.mainMemorySize * 0.3) / t2.blockSize) * t2.blockSize;
			final int outputBufferSize = ((int)(SizeConstants.MAIN_MEMORY_SIZE * 0.4) / blockSize) * blockSize;
			
			relationBufferArray = new Buffer[2];
			relationBufferArray[0] = createAndFillRelationBuffer(sortedT1, t1BufferSize, t1.blockSize);
			relationBufferArray[1] = createAndFillRelationBuffer(sortedT2, t2BufferSize, t2.blockSize);
			
			// Create output buffer
			outputBuffer = ByteBuffer.allocate(outputBufferSize);
						
			int i;
			final int outputBufferTuples = outputBuffer.capacity() / tupleSize;
			boolean isAnyRelationReadComplete = false;
			
			final BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File (FileConstants.OUTPUT_FILE_PATH + FileConstants.SBJ_STUDENT_GPA_FILE_NAME)));
			final FileReader reader = new FileReader(new File(FileConstants.GRADE_PROPERTY_FILE_PATH));  
		    final Properties p = new Properties();  
		    p.load(reader); 
			
		    double creditSum = 0, gradeSum = 0;
		    String prevTuple = "";
		    
			while (true) {
				i = outputBufferTuples;
				while (true) {
					final int result = relationBufferArray[0].compareTo(relationBufferArray[1], SizeConstants.KEY_SIZE);

					if (result == -1) {
						relationBufferArray[0].getBuffer().position(relationBufferArray[0].getBuffer().position() + t1.tupleSize);
						if(prevTuple != null) {
							calculateAndWriteGPA(bufferedWriter, prevTuple.substring(0, 8), gradeSum, creditSum);
							creditSum = 0;
							gradeSum = 0;
							prevTuple = null;
						}
					} else if (result == 1) {
						relationBufferArray[1].getBuffer().position(relationBufferArray[1].getBuffer().position() + t2.tupleSize);
						if(prevTuple != null) {
							calculateAndWriteGPA(bufferedWriter, prevTuple.substring(0, 8), gradeSum, creditSum);
							creditSum = 0;
							gradeSum = 0;
							prevTuple = null;
						}
					} else {
						byte[] tuple = new byte[tupleSize];
						relationBufferArray[0].getBuffer().get(tuple, 0, t1.tupleSize - 1);
						relationBufferArray[0].getBuffer().position(relationBufferArray[0].getBuffer().position() - t1.tupleSize + 1);
						
						relationBufferArray[1].getBuffer().position(relationBufferArray[1].getBuffer().position() + SizeConstants.KEY_SIZE);
						relationBufferArray[1].getBuffer().get(tuple, 100, t2.tupleSize - SizeConstants.KEY_SIZE);
						outputBuffer.put(tuple);
						--i;
						final String currentTuple = new String(tuple);
						
						if(prevTuple == null || prevTuple.isEmpty()) {
							prevTuple = currentTuple;
						}
						
						if(prevTuple.substring(0, SizeConstants.KEY_SIZE).equals(currentTuple.substring(0, SizeConstants.KEY_SIZE))) {
							prevTuple = currentTuple;
							final double credits = Double.parseDouble(prevTuple.substring(tupleSize-7, tupleSize-5).trim());
							final double gradePoints = Double.parseDouble(p.getProperty(prevTuple.substring(tupleSize-5, tupleSize-1).trim()));
							gradeSum += (gradePoints * credits);
							creditSum += credits;
						} else {
							calculateAndWriteGPA(bufferedWriter, prevTuple.substring(0, 8), gradeSum, creditSum);
							creditSum = 0;
							gradeSum = 0;
							prevTuple = currentTuple;
						}
					}

					boolean isBufferRefillSuccessful = refillRelationBuffer(relationBufferArray[0], t1.blockSize);
					if(!isBufferRefillSuccessful) {
						isAnyRelationReadComplete = true;
						break;
					}
					
					isBufferRefillSuccessful = refillRelationBuffer(relationBufferArray[1], t2.blockSize);
					if(!isBufferRefillSuccessful) {
						isAnyRelationReadComplete = true;
						break;
					}
					
					if(i == 0){
						final int bytesWritten = writeOutputBuffer(outputChannel);
						joinWriteCount += bytesWritten / blockSize;
						if(bytesWritten % blockSize != 0) {
							joinWriteCount++;
						}
						i = outputBufferTuples;
					}
				}
				
				if(prevTuple != null) {
					calculateAndWriteGPA(bufferedWriter, prevTuple.substring(0, 8), gradeSum, creditSum);
					creditSum = 0;
					gradeSum = 0;
					prevTuple = null;
				}
				
				if (isAnyRelationReadComplete) {
					break;
				}
			}
			bufferedWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return joinReadCount + joinWriteCount;
	}
	
	private void calculateAndWriteGPA(final BufferedWriter writer, final String tuple, final double gradeSum, final double creditSum) throws IOException {
		final double gpa = CommonUtility.round(gradeSum / creditSum, 2);
		writer.write(tuple + " : " + gpa);
		writer.newLine();
	}
	
	private Buffer createAndFillRelationBuffer(final File relation, final int bufferSize, final int blockSize) {
		try {
			//Create Buffer & Read given relation.
			final FileChannel t1SortedFileChannel = FileChannel.open(Paths.get(relation.getPath()), EnumSet.of(StandardOpenOption.READ));
			final Buffer relationBuffer = new Buffer(ByteBuffer.allocate(bufferSize), 0, relation.length() -1, t1SortedFileChannel);
			relationBuffer.readBuffer();

			//Calculate initial total read count for given relation.
			incrementJoinReadCount(relationBuffer, blockSize);
			
			return relationBuffer;
		} catch (IOException e) {
			System.out.println("The following error occurred during initial relation buffer load: " + e.getMessage());
			return null;
		}
	}
	
	private boolean refillRelationBuffer(final Buffer relationBuffer, final int blockSize) {
		boolean isRefillSuccessful = true;
		if (relationBuffer.getBuffer().position() == relationBuffer.getBuffer().limit()) {
			if (!relationBuffer.readBuffer()) {
				isRefillSuccessful = false; 
			} else {
				incrementJoinReadCount(relationBuffer, blockSize);
			}
		}
		return isRefillSuccessful;
	}
	
	private void incrementJoinReadCount(final Buffer relationBuffer, final int blockSize) {
		final int bytesRead = relationBuffer.getBuffer().limit() - relationBuffer.getBuffer().position();
		joinReadCount += bytesRead / blockSize;
		if(bytesRead % blockSize != 0) {
			joinReadCount++;
		}
	}
	
}