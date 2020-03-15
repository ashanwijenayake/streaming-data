package Nlp;

import opennlp.tools.langdetect.*;
import opennlp.tools.util.*;

import java.io.File;
import java.io.IOException;

/**
 * The class contains the logic to predict the sentiment for a tweet.
 * @author ashanw
 */
public class SentimentAnalyzer {

    private static LanguageDetector languageDetector;

    static {
        try {
            InputStreamFactory dataIn = new MarkableFileInputStreamFactory(new File("src/main/resources/sentiment.txt"));
            ObjectStream lineStream = new PlainTextByLineStream(dataIn, "UTF-8");
            LanguageDetectorSampleStream sampleStream = new LanguageDetectorSampleStream(lineStream);
            TrainingParameters params = new TrainingParameters();
            params.put(TrainingParameters.ITERATIONS_PARAM, 100);
            params.put(TrainingParameters.CUTOFF_PARAM, 5);
            params.put("DataIndexer", "TwoPass");
            params.put(TrainingParameters.ALGORITHM_PARAM, "NAIVEBAYES");
            LanguageDetectorModel languageDetectorModel = LanguageDetectorME.train(sampleStream, params, new LanguageDetectorFactory());
            languageDetector = new LanguageDetectorME(languageDetectorModel);

        } catch (IOException ex) {
            languageDetector = null;
        }
    }

    /**
     * The following method is used to predict the sentiment.
     * @param tweet tweet retrieved from twitter.
     * @return sentiment value. 1 or 0
     */
    public static int predictSentiment(String tweet) {
        if(languageDetector == null) {
            return -1;
        } else {
            Language[] languages = languageDetector.predictLanguages(tweet);
            return Integer.parseInt(languages[0].getLang());
        }
    }
}