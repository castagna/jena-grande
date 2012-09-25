

                          J e n a    G r a n d e


  Jena Grande is a collection of utilities, experiments and examples on 
  how to use MapReduce, Pig, HBase or Giraph to process data in RDF format.

  RDF data model is a labelled directed multigraph. URIs are used to give 
  globally unique names to nodes and labelled links (a.k.a. properties in
  the RDF lingo). A destination node (a.k.a. an object in an RDF statement) 
  can be an attribute value (a.k.a. literal). Nodes can also be unnamed 
  (a.k.a. blank nodes).

  Apache Jena is a Java library, currently in incubation, which can help 
  you parsing, storing and querying RDF data.  

  This is to be considered experimental and work in progress. If you want 
  to share your experience on processing RDF data with any of the Hadoop 
  ecosystem's projects, please, do fork this and send your pull requests.

  If the graph you need to process isn't RDF, some of the tricks shared
  here might be useful to you for other type of graphs.


  Requirements
  ------------

  This is how to get Apache Giraph and install it in your Maven local repo:

    svn co https://svn.apache.org/repos/asf/giraph/trunk/ apache-giraph
    cd apache-giraph
    mvn -P hadoop_2.0.0 install

  ...


  Have fun!
  
  -- Paolo



