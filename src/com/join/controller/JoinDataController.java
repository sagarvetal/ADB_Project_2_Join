package com.join.controller;

import java.io.File;

import com.join.service.FileService;
import com.join.service.JoinService;
import com.join.utility.CommonUtility;
import com.join.utility.FileConstants;
import com.join.utility.SizeConstants;

public class JoinDataController {

	public static void main(String[] args) {

		try {
			int sortIO = 0;
			final FileService fileService = new FileService();
			fileService.deleteOldFiles(FileConstants.SUBLISTS_FILE_PATH);
			fileService.deleteOldFiles(FileConstants.OUTPUT_FILE_PATH);
			
			System.out.println("####==================== WELCOME TO JOIN CONSOLE ====================####");
			System.out.println("\nMemory Size: " + CommonUtility.getMemorySizeInMB() + " MB\n");
			
			System.out.println("************ Main Memory Details ************");
			System.out.println("Available Main Memory for Data Sets : " + CommonUtility.getSizeInMB(SizeConstants.MAIN_MEMORY_SIZE) + " MB");
			System.out.println("Total Main Memory Blocks : " + CommonUtility.getMainMemoryBlocks(SizeConstants.MAIN_MEMORY_SIZE, SizeConstants.BLOCK_SIZE));
			
			System.out.println("\n************ Relation T1 ************");
			final long startTimeforSortBasedJoin = System.nanoTime();
			JoinService relation1 = new JoinService(FileConstants.INPUT_FILE_PATH + FileConstants.RELATION_T1, SizeConstants.T1_TUPLE_SIZE, SizeConstants.T1_BLOCK_SIZE);
			sortIO += relation1.sort();
			sortIO += relation1.merge();
			
			System.out.println("\n************ Relation T2 ************");
			JoinService relation2 = new JoinService(FileConstants.INPUT_FILE_PATH + FileConstants.RELATION_T2, SizeConstants.T2_TUPLE_SIZE, SizeConstants.T2_BLOCK_SIZE);
			sortIO += relation2.sort();
			sortIO += relation2.merge();
			
			System.out.println("\n************** Sort-Based Join Result ***************");
			JoinService joinService = new JoinService();
			final int joinDiskIO = joinService.sortBasedJoin(relation1, relation2);
			final long endTimeforSortBasedJoin = System.nanoTime();
			final File sortJoinResult = new File(FileConstants.OUTPUT_FILE_PATH + FileConstants.SORT_BASED_JOIN_FILE_NAME);

			System.out.println("Tuples in Join Result : " + (sortJoinResult.length() / SizeConstants.JOIN_TUPLE_SIZE));
			System.out.println("Join Result Size : " + CommonUtility.getSizeInMB(sortJoinResult.length()) + " MB");
			System.out.println("Total Disk I/O : " + (sortIO+joinDiskIO));
			System.out.println("Total Execution time: " + CommonUtility.round((float) (endTimeforSortBasedJoin - startTimeforSortBasedJoin) / 1000000000, 2) + " seconds");
			
			relation1=null;
			relation2=null;
			joinService=null;
			System.gc();
			
			System.out.println("\n************** Nested Loop Join Result ***************");
			final long startTimeForNLJ = System.nanoTime();
			final JoinService join = new JoinService();
			final int nestedLoopJoinIO = join.nestedLoopJoin();
			final long endTimeforNLJ = System.nanoTime();
			final File nestedJoinResult = new File(FileConstants.OUTPUT_FILE_PATH + FileConstants.NESTED_LOOP_JOIN_FILE_NAME);
			
			System.out.println("Tuples in Join Result : " + (nestedJoinResult.length() / SizeConstants.JOIN_TUPLE_SIZE));
			System.out.println("Join Result Size : " + CommonUtility.getSizeInMB(nestedJoinResult.length()) + " MB");
			System.out.println("Total Disk I/O : " + nestedLoopJoinIO);
			System.out.println("Total Execution time: " + CommonUtility.round((float) (endTimeforNLJ - startTimeForNLJ) / 1000000000, 2) + " seconds");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
