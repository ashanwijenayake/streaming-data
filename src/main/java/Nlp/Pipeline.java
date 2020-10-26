package nlp;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Pipeline {

    public static void init(String paragraph) throws IOException {

        //Sentence detection
        InputStream sentenceInputStream = Pipeline.class.getResourceAsStream("/en-sent.bin");
        SentenceModel model = new SentenceModel(sentenceInputStream);
        SentenceDetectorME sentenceDetector = new SentenceDetectorME(model);
        List<String> sentenceList = Arrays.asList(sentenceDetector.sentDetect(paragraph));

        //Tokenize
        InputStream tokenInputStream = Pipeline.class.getResourceAsStream("/en-token.bin");
        TokenizerModel tokenizerModel = new TokenizerModel(tokenInputStream);
        final TokenizerME tokenizer = new TokenizerME(tokenizerModel);
        List<String> tokensArrayList = new ArrayList<>();
        sentenceList.forEach(sentence -> tokensArrayList.addAll(Arrays.asList(tokenizer.tokenize(sentence))));

        //Named entity recognition
        InputStream neInputStream = Pipeline.class.getResourceAsStream("/en-ner-person.bin");
        TokenNameFinderModel tokenNameFinderModel = new TokenNameFinderModel(neInputStream);
        NameFinderME nameFinderME = new NameFinderME(tokenNameFinderModel);
        List<Span> spans = Arrays.asList(nameFinderME.find(tokensArrayList.stream().toArray(String[]::new)));

        //POS tagging
        InputStream inputStreamPOSTagger = Pipeline.class.getResourceAsStream("/en-pos-maxent.bin");
        POSModel posModel = new POSModel(inputStreamPOSTagger);
        POSTaggerME posTagger = new POSTaggerME(posModel);
        String tags[] = posTagger.tag(tokensArrayList.stream().toArray(String[]::new));
    }
}
