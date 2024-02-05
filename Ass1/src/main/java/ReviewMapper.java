
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
//import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class ReviewMapper {
    
    private Properties sentimentProps;
    private StanfordCoreNLP sentimentPipeline;
    private Properties entityProps;
    private StanfordCoreNLP NERPipeline;

    public ReviewMapper() {
        sentimentProps = new Properties();
        sentimentProps.put("annotators", "tokenize, ssplit, parse, sentiment");
        sentimentPipeline = new StanfordCoreNLP(sentimentProps);
        entityProps = new Properties();
        entityProps.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        NERPipeline = new StanfordCoreNLP(entityProps);
    }

    public ProcessedReview process(Review review) {
        List<String> entities = getEntities(review.getText());
        int sentiment = findSentiment(review.getText());
        return new ProcessedReview(review.getLink(), convertSentimentRatingToColor(sentiment) , entities, isSarcasem(review.getRating(), sentiment));
    }

    private Color convertSentimentRatingToColor(int sentiment) {
            try{
                switch (sentiment) {
                    case 0:
                        return Color.DARK_RED;
                    case 1:
                        return Color.RED;
                    case 2:
                        return Color.BLACK;
                    case 3:
                        return Color.LIGHT_GREEN;
                    case 4:
                        return Color.DARK_GREEN;
                    default:
                        throw new Exception("impossible sentiment rating: " + sentiment);
                
                }
            } catch(Exception e) {
                System.out.println(e.getMessage());
                return null;
            }

    }

    private boolean isSarcasem(long rating, int sentiment) {
        if(sentiment>=3 && rating<=2)
            return true;
        else if(sentiment <= 1 && rating >= 4)
            return true;
        else
            return false;
    }

    private List<String> getEntities(String review){
            List<String> entities = new LinkedList<String>();
            // create an empty Annotation just with the given text
            Annotation document = new Annotation(review);
            // run all Annotators on this text
            NERPipeline.annotate(document);
            // these are all the sentences in this document
            // a CoreMap is essentially a Map that uses class objects as keys and has values with
            //custom types
            List<CoreMap> sentences = document.get(SentencesAnnotation.class);
            for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
                for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                    String word = token.get(TextAnnotation.class);
                    // this is the NER label of the token
                    String ne = token.get(NamedEntityTagAnnotation.class);
                    if(ne.equals("LOCATION") || ne.equals("ORGANIZATION") || ne.equals("PERSON") )
                        entities.add(word);
                    System.out.println("\t-" + word + ":" + ne);
                }
            }
            return entities;
        }
        
        private int findSentiment(String review) {
            int mainSentiment = 0;
            if (review!= null && review.length() > 0) {
                int longest = 0;
                Annotation annotation = sentimentPipeline.process(review);
                    for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                    Tree tree = sentence.get(
                    SentimentAnnotatedTree.class); // changed from original code in the assignment
                    int sentiment = edu.stanford.nlp.neural.rnn.RNNCoreAnnotations.getPredictedClass(tree); // changed from original code in the assignment
                    String partText = sentence.toString();
                    if (partText.length() > longest) {
                        mainSentiment = sentiment;
                        longest = partText.length();
                    }
                    }
            }
            return mainSentiment;
        }

}
