import edu.stanford.nlp.coref.CorefCoreAnnotations;
import edu.stanford.nlp.coref.data.CorefChain;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Created by Mayanka on 13-Jun-16.
 */
public class Main {
    public static void main(String args[]) {
        // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        String text =
                "Certain subgroups of youth are at high risk for depression and elevated depressive symptoms, and experience limited access to quality mental health care. Examples are socioeconomically disadvantaged, racial/ ethnic minority, and sexual minority youth. Research shows that there are efficacious interventions to prevent youth depression and depressive symptoms. These preventive interventions have the potential to play a key role in addressing these mental health disparities by reducing youth risk factors and enhancing protective factors. However, there are comparatively few preventive interventions directed specifically to these vulnerable subgroups, and sample sizes of diverse subgroups in general prevention trials are often too low to assess whether preventive interventions work equally well for vulnerable youth compared to other youth. In this paper, we describe the importance and need for scientific equity, or equality and fairness in the amount of scientific knowledge produced to understand the potential solutions to such health disparities. We highlight possible strategies for promoting scientific equity, including the following: increasing the number of prevention research participants from vulnerable subgroups, conducting more data synthesis analyses and implementation science research, disseminating preventive interventions that are efficacious for vulnerable youth, and increasing the diversity of the prevention science research workforce. These strategies can increase the availability of research evidence to determine the degree to which preventive interventions can help address mental health disparities. Although this paper utilizes the prevention of youth depression as an illustrative case example, the concepts are applicable to other health outcomes for which there are disparities, such as substance use and obesity.";


        Annotation document = new Annotation(text);

// run all Annotators on this text
        pipeline.annotate(document);

        // these are all the sentences in this document
// a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
        List verb = new ArrayList();
        List noun = new ArrayList();
        List NER = new ArrayList();


        for (CoreMap sentence : sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {

                System.out.println("\n" + token);

                // this is the text of the token
                String word = token.get(CoreAnnotations.TextAnnotation.class);
                System.out.println("Text Annotation");
                System.out.println(token + ":" + word);
                // this is the POS tag of the token

                String lemma = token.get(CoreAnnotations.LemmaAnnotation.class);
                System.out.println("Lemma Annotation");
                System.out.println(token + ":" + lemma);
                // this is the Lemmatized tag of the token


                String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
                System.out.println("Parts of Speech");
                System.out.println(token + ":" + pos);

                // this is the NER label of the token
                String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
                System.out.println("Named Entity Recognition");
                System.out.println(token + ":" + ne);

                System.out.println("\n");

                String v = "V";
                String n = "N";
                String ner = "0";
                String pos_s = String.valueOf(pos.charAt(0));
                String pos_n = String.valueOf(ne.charAt(0));
                System.out.println(pos_n.compareTo(ner));

                if (pos_s.compareTo(v)==0) {
                    verb.add(word);
                }
                if (pos_s.compareTo(n)==0) {
                    noun.add(word);
                }
                if (pos_n.compareTo(ner)!=31) {
                    NER.add(word);
                }

                // this is the parse tree of the current sentence
                System.out.println("Parse Tree of the Current Sentence:");
                Tree tree = sentence.get(TreeCoreAnnotations.TreeAnnotation.class);
                System.out.println(tree);
                System.out.println("\n");
                // this is the Stanford dependency graph of the current sentence

                System.out.println("Stanford Dependency Graph & Co-referencing:");
                Map<Integer, CorefChain> graph =
                        document.get(CorefCoreAnnotations.CorefChainAnnotation.class);
                System.out.println(graph.values().toString());
            }

        }
        System.out.println("number of verb = " + verb.size()+ ";" + "pos: verb = " + verb);
        System.out.println("number of noun = " + noun.size()+ ";" + "pos: noun = " + noun);
        System.out.println("number of NER = " + NER.size()+ ";" + "NER = " + NER);
    }
}