package com.pereira.ml.emailspamdetector;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithm;
import org.apache.spark.mllib.regression.GeneralizedLinearModel;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

public class ClassificationAlgorithm implements Serializable {
	
	private int featureSize;
	private DataUtil dataUtil;
	private Map<String, Integer> vocabulary;
	private GeneralizedLinearAlgorithm model;
	private transient JavaSparkContext sparkContext;
	private GeneralizedLinearModel linearModel;

	public ClassificationAlgorithm(int featureSize, GeneralizedLinearAlgorithm model) {
        this.featureSize = featureSize;
        dataUtil = new DataUtil(featureSize);
        this.model = model;
    }
	
	public double test(String emailString) throws IOException {
        Email email = dataUtil.prepareEmailForTesting(emailString);
        return linearModel.predict(transformToFeatureVector(email, vocabulary));
    }
	
	public MulticlassMetrics execute() throws Exception {
        vocabulary = dataUtil.createVocabulary();
        List<LabeledPoint> labeledPoints = convertToLabelPoints();
        sparkContext = createSparkContext();
        JavaRDD<LabeledPoint> labeledPointJavaRDD = sparkContext.parallelize(labeledPoints);
        //split random training and test data in 80:20 ratio
        JavaRDD<LabeledPoint>[] splits = labeledPointJavaRDD.randomSplit(new double[]{0.8, 0.2}, 11L);
        JavaRDD<LabeledPoint> training = splits[0].cache();
        JavaRDD<LabeledPoint> test = splits[1];


        linearModel = model.run(training.rdd());

        JavaRDD<Tuple2<Object, Object>> predictionAndLabels = test.map(
                (Function<LabeledPoint, Tuple2<Object, Object>>) p -> {
                    Double prediction = linearModel.predict(p.features());
                    return new Tuple2<>(prediction, p.label());
                }
        );

        return new MulticlassMetrics(predictionAndLabels.rdd());
    }
	
	public void dispose() {
        sparkContext.close();
    }
	
	/**
	 * Asim: prepare labeled data points (with 0/1 if spam/notspam, and the corresponding feature vector) ex. 0, <vector>
	 * @return
	 * @throws Exception
	 */
	private List<LabeledPoint> convertToLabelPoints() throws Exception {
        ArrayList<Email> emails = new ArrayList<>();
        emails.addAll(dataUtil.filesToEmailWords("allInOneSpamBase/spam"));
        emails.addAll(dataUtil.filesToEmailWords("allInOneSpamBase/hard_ham"));
        emails.addAll(dataUtil.filesToEmailWords("allInOneSpamBase/easy_ham"));
        emails.addAll(dataUtil.filesToEmailWords("allInOneSpamBase/easy_ham_2"));
        emails.addAll(dataUtil.filesToEmailWords("allInOneSpamBase/spam_2"));
        return emails.stream().parallel().map(e -> new LabeledPoint(e.isSpam() == true ? 1 : 0, transformToFeatureVector(e, vocabulary))).collect(Collectors.toList());
    }
	
	/**
	 * Asim: This gets a matrix for each words in the email that matches the vocabulary and gets the matrix of count
	 * where key of spam vocab matches the count (same index)
	 * @param email
	 * @param vocabulary
	 * @return
	 */
	private Vector transformToFeatureVector(Email email, Map<String, Integer> vocabulary) {
        List<String> words = email.getWords();
        HashMap<String, Integer> countWords = dataUtil.countWords(words);
        double[] features = new double[featureSize];
        for (Map.Entry<String, Integer> word : countWords.entrySet()) {
            Integer index = vocabulary.get(word.getKey());
            if (index != null) {
                features[index] = word.getValue();
            }
        }
        return Vectors.dense(features);
    }
	
	private JavaSparkContext createSparkContext() {
        SparkConf conf = new SparkConf().setAppName("Email Spam Detection").setMaster("local[*]");
        return new JavaSparkContext(conf);
    }

}
