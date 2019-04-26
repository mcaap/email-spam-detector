package com.pereira.ml.emailspamdetector;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

public class DataUtilTest {

	@Test
	public void testFilesToWords() {
		try {
			DataUtil util = new DataUtil();
			List<String> filesToWords = util.filesToWords("allInOneSpamBase/spam/00001.7848dde101aa985090474a91ec93fcf0");
			System.out.println(filesToWords);
			assertEquals(183, filesToWords.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
