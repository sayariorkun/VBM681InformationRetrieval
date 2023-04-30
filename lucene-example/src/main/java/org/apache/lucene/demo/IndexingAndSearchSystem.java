package org.apache.lucene.demo;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.AfterEffectB;
import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BasicModelG;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.IndriDirichletSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.NormalizationH1;
import org.apache.lucene.search.similarities.NormalizationH2;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.commons.io.IOUtils;

public class IndexingAndSearchSystem {

	public static void main(String[] args) throws IOException, ParseException {

		// change the following input and output paths to your local ones
		  String pathCorpus = "C:\\Users\\ORKUN\\Downloads\\example_corpus.gz";
		  String pathIndex = "C:\\Users\\ORKUN\\Downloads\\example_index_lucene.tar";
		  
		  Directory dir = FSDirectory.open( new File( pathIndex ).toPath() );
		  
		  // Analyzer specifies options for text tokenization and normalization (e.g., stemming, stop words removal, case-folding)
		  Analyzer analyzer = new Analyzer() {
		      @Override
		      protected TokenStreamComponents createComponents( String fieldName ) {
		          // Step 1: tokenization (Lucene's StandardTokenizer is suitable for most text retrieval occasions)
		          TokenStreamComponents ts = new TokenStreamComponents( new StandardTokenizer() );
		          // Step 2: transforming all tokens into lowercased ones (recommended for the majority of the problems)
		          ts = new TokenStreamComponents( ts.getSource(), new LowerCaseFilter( ts.getTokenStream() ) );
		          // Step 3: whether to remove stop words (unnecessary to remove stop words unless you can't afford the extra disk space)
		          // Uncomment the following line to remove stop words
		          // ts = new TokenStreamComponents( ts.getSource(), new StopFilter( ts.getTokenStream(), EnglishAnalyzer.ENGLISH_STOP_WORDS_SET ) );
		          // Step 4: whether to apply stemming
		          // Uncomment one of the following two lines to apply Krovetz or Porter stemmer (Krovetz is more common for IR research)
		          ts = new TokenStreamComponents( ts.getSource(), new KStemFilter( ts.getTokenStream() ) );
		          // ts = new TokenStreamComponents( ts.getSource(), new PorterStemFilter( ts.getTokenStream() ) );
		          return ts;
		      }
		  };
		  
		  IndexWriterConfig config = new IndexWriterConfig( analyzer );
		  // Note that IndexWriterConfig.OpenMode.CREATE will override the original index in the folder
		  config.setOpenMode( IndexWriterConfig.OpenMode.CREATE );
		  // Lucene's default BM25Similarity stores document field length using a "low-precision" method.
		  // Use the BM25SimilarityOriginal to store the original document length values in index.
		  
		  //config.setSimilarity( new DFRSimilarity() );
		  config.setSimilarity(new DFRSimilarity(new BasicModelG(), new AfterEffectB(), new NormalizationH2()));
		  //config.setSimilarity(new ClassicSimilarity());
		  //config.setSimilarity(new LMDirichletSimilarity());
		  
		  IndexWriter ixwriter = new IndexWriter( dir, config );
		  
		  // This is the field setting for metadata field (no tokenization, searchable, and stored).
		  FieldType fieldTypeMetadata = new FieldType();
		  fieldTypeMetadata.setOmitNorms( true );
		  fieldTypeMetadata.setIndexOptions( IndexOptions.DOCS );
		  fieldTypeMetadata.setStored( true );
		  fieldTypeMetadata.setTokenized( false );
		  fieldTypeMetadata.freeze();
		  
		  // This is the field setting for normal text field (tokenized, searchable, store document vectors)
		  FieldType fieldTypeText = new FieldType();
		  fieldTypeText.setIndexOptions( IndexOptions.DOCS_AND_FREQS_AND_POSITIONS );
		  fieldTypeText.setStoreTermVectors( true );
		  fieldTypeText.setStoreTermVectorPositions( true );
		  fieldTypeText.setTokenized( true );
		  fieldTypeText.setStored( true );
		  fieldTypeText.freeze();
		  
		  // You need to iteratively read each document from the example corpus file,
		  // create a Document object for the parsed document, and add that
		  // Document object by calling addDocument().
		  
		  // Well, the following only works for small text files. DO NOT follow this part for large dataset files.
		  InputStream instream = new GZIPInputStream( new FileInputStream( pathCorpus ) );
		  String corpusText = new String( IOUtils.toByteArray(instream), "UTF-8" );
		  instream.close();
		  
		  Pattern pattern = Pattern.compile(
		      "<DOC>.+?<DOCNO>(.+?)</DOCNO>.+?<TITLE>(.+?)</TITLE>.+?<AUTHOR>(.+?)</AUTHOR>.+?<SOURCE>(.+?)</SOURCE>.+?<TEXT>(.+?)</TEXT>.+?</DOC>",
		      Pattern.CASE_INSENSITIVE + Pattern.MULTILINE + Pattern.DOTALL
		  );
		  
		  Matcher matcher = pattern.matcher( corpusText );
		  
		  while ( matcher.find() ) {
		  
		      String docno = matcher.group( 1 ).trim();
		      String title = matcher.group( 2 ).trim();
		      String author = matcher.group( 3 ).trim();
		      String source = matcher.group( 4 ).trim();
		      String text = matcher.group( 5 ).trim();
		      
		      // Create a Document object
		      Document d = new Document();
		      // Add each field to the document with the appropriate field type options
		      d.add( new Field( "docno", docno, fieldTypeMetadata ) );
		      d.add( new Field( "title", title, fieldTypeText ) );
		      d.add( new Field( "author", author, fieldTypeText ) );
		      d.add( new Field( "source", source, fieldTypeText ) );
		      d.add( new Field( "text", text, fieldTypeText ) );
		      // Add the document to the index
		      System.out.println( "indexing document " + docno );
		      ixwriter.addDocument( d );
		  }
		  
		  // remember to close both the index writer and the directory
		  ixwriter.close();
		  dir.close();
		  
		// modify to your index path
		  String pathIndex1 = "C:\\Users\\ORKUN\\Downloads\\example_index_lucene.tar"; 

		  // First, open the directory
		  Directory dir1 = FSDirectory.open( new File( pathIndex1 ).toPath() );

		  // Then, open an IndexReader to access your index
		  IndexReader index = DirectoryReader.open( dir1 );

		  // Now, start working with your index using the IndexReader object

		  int indHelp = index.numDocs(); // just an example: get the number of documents in the index
		  System.out.println(indHelp);
		  
		  
		  IndexReader index1 = DirectoryReader.open( dir1 );

		// the name of the field storing external IDs (docnos)
		String fieldName = "docno";

		int docid = 5;
		String docNo = LuceneUtils.getDocno( index1, fieldName, docid ); // get the docno for the internal docid = 5

		String docno = "ACM-1835461";
		int internalID = LuceneUtils.findByDocno( index1, fieldName, docno ); // get the internal docid for docno "ACM-1835461"
		
		System.out.println(docNo);
		System.out.println(internalID);
		// Remember to close both the IndexReader and the Directory after use 
		  index.close();
		  dir1.close();
		  
		  String pathIndex11 = "C:\\Users\\ORKUN\\Downloads\\example_index_lucene.tar";

		// Let's just retrieve the posting list for the term "reformulation" in the "text" field
		String field = "text";
		String term = "reformulation";

		Directory dir11 = FSDirectory.open( new File( pathIndex11 ).toPath() );
		IndexReader index11 = DirectoryReader.open( dir11 );

		// The following line reads the posting list of the term in a specific index field.
		// You need to encode the term into a BytesRef object,
		// which is the internal representation of a term used by Lucene.
		System.out.printf( "%-10s%-15s%-6s\n", "DOCID", "DOCNO", "FREQ" );
		PostingsEnum posting = MultiTerms.getTermPostingsEnum( index11, field, new BytesRef( term ), PostingsEnum.FREQS );
		if ( posting != null ) { // if the term does not appear in any document, the posting object may be null
		    int docid1;
		    // Each time you call posting.nextDoc(), it moves the cursor of the posting list to the next position
		    // and returns the docid of the current entry (document). Note that this is an internal Lucene docid.
		    // It returns PostingsEnum.NO_MORE_DOCS if you have reached the end of the posting list.
		    while ( ( docid1 = posting.nextDoc() ) != PostingsEnum.NO_MORE_DOCS ) {
		        String docno1 = LuceneUtils.getDocno( index11, "docno", docid1 );
		        int freq = posting.freq(); // get the frequency of the term in the current document
		        System.out.printf( "%-10d%-15s%-6d\n", docid1, docno1, freq );
		    }
		}

		index11.close();
		dir11.close();
		
		String pathIndex111 = "C:\\Users\\ORKUN\\Downloads\\example_index_lucene.tar";

		// Let's just retrieve the posting list for the term "reformulation" in the "text" field
		String field1 = "text";
		String term1 = "reformulation";

		Directory dir111 = FSDirectory.open( new File( pathIndex111 ).toPath() );
		IndexReader index111 = DirectoryReader.open( dir111 );

		// we also print out external ID
		Set<String> fieldset = new HashSet<>();
		fieldset.add( "docno" );

		// The following line reads the posting list of the term in a specific index field.
		// You need to encode the term into a BytesRef object,
		// which is the internal representation of a term used by Lucene.
		System.out.printf( "%-10s%-15s%-10s%-20s\n", "DOCID", "DOCNO", "FREQ", "POSITIONS" );
		PostingsEnum posting1 = MultiTerms.getTermPostingsEnum( index111, field1, new BytesRef( term1 ), PostingsEnum.POSITIONS );
		if ( posting1 != null ) { // if the term does not appear in any document, the posting object may be null
		    int docid1;
		    // Each time you call posting.nextDoc(), it moves the cursor of the posting list to the next position
		    // and returns the docid of the current entry (document). Note that this is an internal Lucene docid.
		    // It returns PostingsEnum.NO_MORE_DOCS if you have reached the end of the posting list.
		    while ( ( docid1 = posting1.nextDoc() ) != PostingsEnum.NO_MORE_DOCS ) {
		        String docno1 = index111.document( docid1, fieldset ).get( "docno" );
		        int freq = posting1.freq(); // get the frequency of the term in the current document
		        System.out.printf( "%-10d%-15s%-10d", docid1, docno1, freq );
		        for ( int i = 0; i < freq; i++ ) {
		            // Get the next occurrence position of the term in the current document.
		            // Note that you need to make sure by yourself that you at most call this function freq() times.
		            System.out.print( ( i > 0 ? "," : "" ) + posting1.nextPosition() );
		        }
		        System.out.println();
		    }
		}

		index111.close();
		dir111.close();
		
		String pathIndex1111 = "C:\\Users\\ORKUN\\Downloads\\example_index_lucene.tar";

		// let's just retrieve the document vector (only the "text" field) for the Document with internal ID=21
		String field11 = "text";
		int docid1 = 21;

		Directory dir1111 = FSDirectory.open( new File( pathIndex1111 ).toPath() );
		IndexReader index1111 = DirectoryReader.open( dir1111 );

		Terms vector = index1111.getTermVector( docid1, field11 ); // Read the document's document vector.

		// You need to use TermsEnum to iterate each entry of the document vector (in alphabetical order).
		System.out.printf( "%-20s%-10s%-20s\n", "TERM", "FREQ", "POSITIONS" );
		TermsEnum terms = vector.iterator();
		PostingsEnum positions = null;
		BytesRef term11;
		while ( ( term11 = terms.next() ) != null ) {
		    
		    String termstr = term11.utf8ToString(); // Get the text string of the term.
		    long freq = terms.totalTermFreq(); // Get the frequency of the term in the document.
		    
		    System.out.printf( "%-20s%-10d", termstr, freq );
		    
		    // Lucene's document vector can also provide the position of the terms
		    // (in case you stored these information in the index).
		    // Here you are getting a PostingsEnum that includes only one document entry, i.e., the current document.
		    positions = terms.postings( positions, PostingsEnum.POSITIONS );
		    positions.nextDoc(); // you still need to move the cursor
		    // now accessing the occurrence position of the terms by iteratively calling nextPosition()
		    for ( int i = 0; i < freq; i++ ) {
		        System.out.print( ( i > 0 ? "," : "" ) + positions.nextPosition() );
		    }
		    System.out.println();
		}

		index1111.close();
		dir1111.close();
		
		String pathIndex11111 = "C:\\Users\\ORKUN\\Downloads\\example_index_lucene.tar";
		String field111 = "text";

		Directory dir11111 = FSDirectory.open( new File( pathIndex ).toPath() );
		IndexReader ixreader5 = DirectoryReader.open( dir11111 );

		// we also print out external ID
		Set<String> fieldset1 = new HashSet<>();
		fieldset1.add( "docno" );

		// The following loop iteratively print the lengths of the documents in the index.
		System.out.printf( "%-10s%-15s%-10s\n", "DOCID", "DOCNO", "Length" );
		for ( int docid11 = 0; docid11 < ixreader5.maxDoc(); docid11++ ) {
		    String docno1 = ixreader5.document( docid11, fieldset1 ).get( "docno" );
		    int doclen = 0;
		    TermsEnum termsEnum = ixreader5.getTermVector( docid11, field ).iterator();
		    while ( termsEnum.next() != null ) {
		        doclen += termsEnum.totalTermFreq();
		    }
		    System.out.printf( "%-10d%-15s%-10d\n", docid11, docno1, doclen );
		}

		ixreader5.close();
		dir11111.close();
		
		String pathIndex111111 = "C:\\Users\\ORKUN\\Downloads\\example_index_lucene.tar";

		// Let's just retrieve the vocabulary of the "text" field
		String field1111 = "text";

		Directory dir111111 = FSDirectory.open( new File( pathIndex ).toPath() );
		IndexReader index11111 = DirectoryReader.open( dir111111 );

		double N = index11111.numDocs();
		double corpusLength = index11111.getSumTotalTermFreq( field1111 );

		System.out.printf( "%-30s%-10s%-10s%-10s%-10s\n", "TERM", "DF", "TOTAL_TF", "IDF", "p(w|c)" );

		// Get the vocabulary of the index.

		Terms voc = MultiTerms.getTerms( index11111, field1111 );
		// You need to use TermsEnum to iterate each entry of the vocabulary.
		TermsEnum termsEnum = voc.iterator();
		BytesRef term111;
		int count = 0;
		while ( ( term111 = termsEnum.next() ) != null ) {
		    count++;
		    String termstr = term111.utf8ToString(); // get the text string of the term
		    int n = termsEnum.docFreq(); // get the document frequency (DF) of the term
		    long freq = termsEnum.totalTermFreq(); // get the total frequency of the term
		    double idf = Math.log( ( N + 1.0 ) / ( n + 1.0 ) ); // well, we normalize N and n by adding 1 to avoid n = 0
		    double pwc = freq / corpusLength;
		    System.out.printf( "%-30s%-10d%-10d%-10.2f%-10.8f\n", termstr, n, freq, idf, pwc );
		    if ( count >= 100 ) {
		        break;
		    }
		}

		index11111.close();
		dir111111.close();
		
		String pathIndex1111111 = "C:\\Users\\ORKUN\\Downloads\\example_index_lucene.tar";

		// Let's just count the IDF and P(w|corpus) for the word "reformulation" in the "text" field
		String field11111 = "text";
		String term1111 = "reformulation";

		Directory dir1111111 = FSDirectory.open( new File( pathIndex1111111 ).toPath() );
		IndexReader index111111 = DirectoryReader.open( dir1111111 );

		int N1 = index111111.numDocs(); // the total number of documents in the index
		int n = index111111.docFreq( new Term( field11111, term1111 ) ); // get the document frequency of the term in the "text" field
		double idf = Math.log( ( N1 + 1.0 ) / ( n + 1.0 ) ); // well, we normalize N and n by adding 1 to avoid n = 0

		System.out.printf( "%-30sN=%-10dn=%-10dIDF=%-8.2f\n", term1111, N1, n, idf );

		long corpusTF = index111111.totalTermFreq( new Term( field11111, term1111 ) ); // get the total frequency of the term in the "text" field
		long corpusLength1 = index111111.getSumTotalTermFreq( field11111 ); // get the total length of the "text" field
		double pwc = 1.0 * corpusTF / corpusLength1;

		System.out.printf( "%-30slen(corpus)=%-10dfreq(%s)=%-10dP(%s|corpus)=%-10.6f\n", term1111, corpusLength1, term1111, corpusTF, term1111, pwc );

		// remember to close the index and the directory
		index111111.close();
		dir1111111.close();
		
		String pathIndex11111111 = "C:\\Users\\ORKUN\\Downloads\\example_index_lucene.tar";
		  
		  // Analyzer specifies options for text tokenization and normalization (e.g., stemming, stop words removal, case-folding)
		  Analyzer analyzer1 = new Analyzer() {
		      @Override
		      protected TokenStreamComponents createComponents( String fieldName ) {
		          // Step 1: tokenization (Lucene's StandardTokenizer is suitable for most text retrieval occasions)
		          TokenStreamComponents ts = new TokenStreamComponents( new StandardTokenizer() );
		          // Step 2: transforming all tokens into lowercased ones (recommended for the majority of the problems)
		          ts = new TokenStreamComponents( ts.getSource(), new LowerCaseFilter( ts.getTokenStream() ) );
		          // Step 3: whether to remove stop words (unnecessary to remove stop words unless you can't afford the extra disk space)
		          // Uncomment the following line to remove stop words
		          // ts = new TokenStreamComponents( ts.getSource(), new StopFilter( ts.getTokenStream(), EnglishAnalyzer.ENGLISH_STOP_WORDS_SET ) );
		          // Step 4: whether to apply stemming
		          // Uncomment one of the following two lines to apply Krovetz or Porter stemmer (Krovetz is more common for IR research)
		          ts = new TokenStreamComponents( ts.getSource(), new KStemFilter( ts.getTokenStream() ) );
		          // ts = new TokenStreamComponents( ts.getSource(), new PorterStemFilter( ts.getTokenStream() ) );
		          return ts;
		      }
		  };
		  
		  String field111111 = "text"; // the field you hope to search for
		  QueryParser parser = new QueryParser( field111111, analyzer1 ); // a query parser that transforms a text string into Lucene's query object
		  
		  String qstr = "query reformulation"; // this is the textual search query
		  Query query = parser.parse( qstr ); // this is Lucene's query object
		  
		  // Okay, now let's open an index and search for documents
		  Directory dir11111111 = FSDirectory.open( new File( pathIndex11111111 ).toPath() );
		  IndexReader index1111111 = DirectoryReader.open( dir11111111 );
		  
		  // you need to create a Lucene searcher
		  IndexSearcher searcher = new IndexSearcher( index1111111 );
		  
		  // make sure the similarity class you are using is consistent with those being used for indexing
		  searcher.setSimilarity( new DFRSimilarity(new BasicModelG(), new AfterEffectB(), new NormalizationH2()));
		  //searcher.setSimilarity( new ClassicSimilarity());
		  //searcher.setSimilarity( new LMDirichletSimilarity());
		  
		  int top = 10; // Let's just retrieve the talk 10 results
		  TopDocs docs = searcher.search( query, top ); // retrieve the top 10 results; retrieved results are stored in TopDocs
		  
		  System.out.printf( "%-10s%-20s%-10s%s\n", "Rank", "DocNo", "Score", "Title" );
		  int rank = 1;
		  for ( ScoreDoc scoreDoc : docs.scoreDocs ) {
		      int docid11 = scoreDoc.doc;
		      double score = scoreDoc.score;
		      String docno1 = LuceneUtils.getDocno( index1111111, "docno", docid11 );
		      String title = LuceneUtils.getDocno( index1111111, "title", docid11 );
		      System.out.printf( "%-10d%-20s%-10.4f%s\n", rank, docno1, score, title );
		      rank++;
		  }
		  
		  // remember to close the index and the directory
		  index1111111.close();
		  dir11111111.close();
	}

}
