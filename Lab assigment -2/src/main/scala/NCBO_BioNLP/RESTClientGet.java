package NCBO_BioNLP;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;

public class RESTClientGet {
    static public String[] getMedWords(String abstract_id) {
        // Bioconcept: We support five kinds of bioconcepts, i.e., Gene, Disease, Chemical, Species, Mutation.
        // When 'BioConcept' is used, all five are included.
        String BioConcept = "BioConcept";

        // Format can be in the following: PubTator (tab-delimited text file), BioC (xml), and JSON
        String Format = "PubTator";

        ArrayList<String> medicalWords = new ArrayList<String>();

        try {
            URL url_Submit;
            url_Submit = new URL("https://www.ncbi.nlm.nih.gov/CBBresearch/Lu/Demo/RESTful/tmTool.cgi/"
                    + BioConcept + "/" + abstract_id + "/" + Format + "/");
            System.out.println(url_Submit);
            HttpURLConnection conn_Submit = (HttpURLConnection) url_Submit.openConnection();
            conn_Submit.setDoOutput(true);

            BufferedReader br_Submit = new BufferedReader(new InputStreamReader(conn_Submit.getInputStream()));
            String line = "";
            int line_ct = 0;
            while ((line = br_Submit.readLine()) != null) {
                String[] word_info = line.split("\t");
                System.out.println(Arrays.toString(word_info));
                // Array must not be empty or contain 1 element of ""
//                if (word_info.length != 0 && !word_info[0].equals("")) {
//                    if (line_ct < 2) {
//                        medicalWords.add(line);
//                    } else {
//                        // Extract PMID and output the word and the genre associated with the word
//                        medicalWords.add(word_info[0] + "\t" + word_info[3] + "\t" + word_info[4]);
//                    }
//                }

                // Array must not not be empty nor contain an empty string
                // Only obtain the medical words, not the Title and Abstract (line_ct >= 2)
                if (word_info.length !=0 && !word_info[0].equals("") && line_ct >= 2) {
                    // Extract PMID and output the word and the genre associated with the word
                    medicalWords.add(word_info[0] + "\t" + word_info[3] + "\t" + word_info[4] + "\n");
                }
                line_ct++;
            }
            conn_Submit.disconnect();

        }
        catch (MalformedURLException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        String[] arr = new String[medicalWords.size()];
        arr = medicalWords.toArray(arr);
        return arr;

    }

}