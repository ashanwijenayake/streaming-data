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

    public static int predictSentiment(String tweet) {
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
            LanguageDetector ld = new LanguageDetectorME(languageDetectorModel);
            Language[] languages = ld.predictLanguages(tweet);
            return Integer.parseInt(languages[0].getLang());
        } catch(IOException ex){

            return -1;
        }
    }
}
