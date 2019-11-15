# Data Modelling Paper - Supplementary material

This repository contains the ancilliary material for the paper "<title>" submitted to Astronomy and computing. It is divied into four sections.
  
  ### Environment confirugation scripts
  This section contains 
  * the scripts to create the virtual environments on the dostributed cluster environment
  * The spark2-submit basic confiurations to submit the jobs
  * The safety-valve.xml configuration file demonstrating how to run vierutl envs across a cluister
  * The Jupyer kernel file to access Spark and the correct virtual environment
  
  ### Table creation notebooks
  This section contains
  * Examples of the SQL ode used to create training set tables pivoted into feature vector arrays
  * Examples of the SQL used to create the calculated fields and time intervals.
  * Examples of the techniques used to pad the feature vectors to a predetermined limit
  * Examples of the code used to augment a training set and instantiate these changes into a table.
  
  ### Table augmentation notebooks
  
  ### Source code for the python analysis programs
  
  ### Miscellaenous 
  
  * Pig compression scripts
  * Bash code to collect the run tine stats for each Spark application
  * Table definitions of the logging tables
  * A comparison of explain plans - sql statement with joins Vs single instantiated table
