package com.pereira.ml.emailspamdetector;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import org.tartarus.snowball.ext.PorterStemmer;

/**
 * Original: https://github.com/klevis/emailSpamFilter/blob/master/src/main/java/ramo/klevis/ml/emailspam/PrepareData.java
 * @author asim
 *
 */
public class DataUtil implements Serializable {
	
	private long featureSize;

    public DataUtil(long featureSize) {
        this.featureSize = featureSize;
    }

    //Testing
	public DataUtil() {
	}
	
	public Email prepareEmailForTesting(String emailString) {
        String formattedEmail = prepareEmail(emailString.toLowerCase());
		List<String> emailTokens = tokenizeIntoWords(formattedEmail);
		return new Email(emailTokens);
    }
	
	public List<Email> filesToEmailWords(String fileName) throws IOException, URISyntaxException {
        URI uri = this.getClass().getResource("/" + fileName).toURI();
        Path start = Paths.get(uri);
        List<Email> collect = Files.walk(start).parallel()
                .filter(Files::isRegularFile)
                .map(file -> {
                    try {

                        return new Email(tokenizeIntoWords(prepareEmail(new String(Files.readAllBytes(file)).toLowerCase())), fileName.contains("spam"));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }).collect(Collectors.toList());

        System.out.println("collect = " + collect.size() + " " + fileName);
        return collect;
    }

	public Map<String, Integer> createVocabulary() throws Exception {
        String first = "allInOneSpamBase/spam";
        String second = "allInOneSpamBase/spam_2";
        List<String> collect1 = filesToWords(first);
        List<String> collect2 = filesToWords(second);

        ArrayList<String> all = new ArrayList<>(collect1);
        all.addAll(collect2);
        HashMap<String, Integer> countWords = countWords(all);

        List<Map.Entry<String, Integer>> sortedVocabulary = countWords.entrySet().stream().parallel().sorted((o1, o2) -> o2.getValue().compareTo(o1.getValue())).collect(Collectors.toList());
        final int[] index = {0};
        return sortedVocabulary.stream().limit(featureSize).collect(Collectors.toMap(e -> e.getKey(), e -> index[0]++));
    }
	
	HashMap<String, Integer> countWords(List<String> all) {
        HashMap<String, Integer> countWords = new HashMap<>();
        for (String s : all) {
            if (countWords.get(s) == null) {
                countWords.put(s, 1);
            } else {
                countWords.put(s, countWords.get(s) + 1);
            }
        }
        return countWords;
    }
	
	List<String> filesToWords(String fileName) throws Exception {
		System.out.println("reading " + fileName);
	    URI uri = getClass().getResource("/" + fileName).toURI();
	    Path start = Paths.get(uri);
	    List<String> collect = Files.walk(start).parallel()
	            .filter(Files::isRegularFile)
	            .flatMap(file -> {
	                try {

	                    return Stream.of(new String(Files.readAllBytes(file)).toLowerCase());
	                } catch (IOException e) {
	                    e.printStackTrace();
	                }
	                return null;
	            }).collect(Collectors.toList());

	    
	    return collect.stream().parallel().flatMap(e -> tokenizeIntoWords(prepareEmail(e)).stream()).collect(Collectors.toList());
	}


	private String prepareEmail(String email) {
		int beginIndex = email.indexOf("\n\n");
		String withoutHeader = email;
		if (beginIndex > 0) {
			withoutHeader = email.substring(beginIndex, email.length());
		}
		String tagsRemoved = withoutHeader.replaceAll("<[^<>]+>", "");
		String numberedReplaced = tagsRemoved.replaceAll("[0-9]+", "XNUMBERX ");
		String urlReplaced = numberedReplaced.replaceAll("(http|https)://[^\\s]*", "XURLX ");
		String emailReplaced = urlReplaced.replaceAll("[^\\s]+@[^\\s]+", "XEMAILX ");
		String dollarReplaced = emailReplaced.replaceAll("[$]+", "XMONEYX ");
		return dollarReplaced;
	}

	private List<String> tokenizeIntoWords(String dollarReplaced) {
		String delim = "[' @$/#.-:&*+=[]?!(){},''\\\">_<;%'\t\n\r\f";
		StringTokenizer stringTokenizer = new StringTokenizer(dollarReplaced, delim);
		List<String> wordsList = new ArrayList<>();
		while (stringTokenizer.hasMoreElements()) {
			String word = (String) stringTokenizer.nextElement();
			String nonAlphaNumericRemoved = word.replaceAll("[^a-zA-Z0-9]", "");
			
//			Word Stemming: Words are reduced to their stemmed form. For example,
//			“discount”, “discounts”, “discounted” and “discounting” are all
//			replaced with “discount”
			PorterStemmer stemmer = new PorterStemmer();
			stemmer.setCurrent(nonAlphaNumericRemoved);
			stemmer.stem();
			String stemmed = stemmer.getCurrent();
			wordsList.add(stemmed);
		}
		return wordsList;
	}

}
