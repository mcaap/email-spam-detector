package com.pereira.ml.emailspamdetector;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;

public class DetectorMain {

	private static final String YOUR_EMAIL_FILE = "your_email.txt";
	private static final int FEATURE_SIZE = 1000;

	public static void main(String[] args) {
//		Logger.getLogger(DetectorMain.class).setLevel(Level.WARN);
		System.setProperty("log4j.configuration", new File("log4j.properties").toURI().toString());
		
		DetectorMain main = new DetectorMain();
		main.train();
	}
	
	void train() {
		System.out.println("training");
		LogisticRegressionWithSGD model = new LogisticRegressionWithSGD();
		ClassificationAlgorithm classificationAlgo = new ClassificationAlgorithm(FEATURE_SIZE, model);
		try {
			long start = System.currentTimeMillis();
			MulticlassMetrics multiclassMetrics = classificationAlgo.execute();
			long time = System.currentTimeMillis() - start;
			System.out.println("Algorithm trained with accuracy : " + multiclassMetrics.accuracy() + " in " + time / 1000 + " seconds");
			
			//Test
			URI uri = new File(YOUR_EMAIL_FILE).toURI();
			String email = new String(Files.readAllBytes(Paths.get(uri)));
//			logger.info("email: " + email);
			double value = classificationAlgo.test(email);
			//if 1d, then its spam, else not spam
			System.out.println("=================");
			System.out.println(value == 1d ? "SPAM" : "Not SPAM");
			System.out.println("=================");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
