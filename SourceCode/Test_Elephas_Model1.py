import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

import os
import math
import copy
import random

import time

from pyspark.ml.feature import StringIndexer, StandardScaler,VectorAssembler,OneHotEncoder
from pyspark.ml import Pipeline

from pyspark import SparkConf,SparkContext
from pyspark.sql import Row, SQLContext, SparkSession
from pyspark.sql.types import *

from pyspark.sql.functions import udf, col, array, lit,monotonically_increasing_id
from pyspark.ml.linalg import Vectors, VectorUDT

from pyspark.sql.functions import rand
from pyspark.sql.types import ArrayType, FloatType,IntegerType, DataType, DoubleType

from pyspark.mllib.evaluation import MulticlassMetrics

from keras import optimizers
from keras.models import Sequential, Model, load_model # model and load_model from Plasticc
from keras.layers import Dense, Dropout, Activation, Layer, Lambda
from keras import backend as K

from elephas.ml_model import ElephasEstimator


from keras.layers import *
from keras.optimizers import Adam, Nadam, SGD
from keras.callbacks import EarlyStopping, ModelCheckpoint, TensorBoard
from keras.utils import to_categorical
from keras.utils import np_utils, generic_utils

from keras.preprocessing.sequence import pad_sequences

from sklearn.model_selection import train_test_split, KFold
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import normalize
from sklearn.metrics import confusion_matrix

import tensorflow as tf
import gc

# ====================================================
# Logging
# ====================================================

from datetime import datetime

import logging


def get_wtable(df):
    
    all_y = np.array(df.select('target').collect(), dtype = 'int32') 

    y_count = np.unique(all_y, return_counts=True)[1]

    wtable = np.ones(len(classes))

    for i in range(0, y_count.shape[0]):
        wtable[i] = y_count[i] / all_y.shape[0]

    return wtable    

def mywloss(y_true,y_pred):
    yc=tf.clip_by_value(y_pred,1e-15,1-1e-15)
    loss=-(tf.reduce_mean(tf.reduce_mean(y_true*tf.log(yc),axis=0)/wtable))
    return loss
    
    
def multi_weighted_logloss(y_ohe, y_p, wtable):
    """
    @author olivier https://www.kaggle.com/ogrellier
    multi logloss for PLAsTiCC challenge
    """
    # Normalize rows and limit y_preds to 1e-15, 1-1e-15
    y_p = np.clip(a=y_p, a_min=1e-15, a_max=1-1e-15)
    # Transform to log
    y_p_log = np.log(y_p)
    # Get the log for ones, .values is used to drop the index of DataFrames
    # Exclude class 99 for now, since there is no class99 in the training set 
    # we gave a special process for that class
    y_log_ones = np.sum(y_ohe * y_p_log, axis=0)
    # Get the number of positives for each class
    nb_pos = y_ohe.sum(axis=0).astype(float)
    nb_pos = wtable

    if nb_pos[-1] == 0:
        nb_pos[-1] = 1

    # Weight average and divide by the number of positives
    class_arr = np.array([class_weight[k] for k in sorted(class_weight.keys())])
    y_w = y_log_ones * class_arr / nb_pos    
    loss = - np.sum(y_w) / np.sum(class_arr)
    return loss / y_ohe.shape[0]

def get_model(train_df, input_dim, size=80):
    def get_meta(x):
        x=x[:,0:10]
        return x
    
    def get_band(x):
        x=x[:,10:266]
        return x
    
    def get_hist(x):
        # x=x[:,266:input_dim]  -- this is before we passed in the shape from input dim - to avoid two select calls
        x=x[:,266:input_dim[0]]
        x=Reshape((8,256))(x)
        x=K.permute_dimensions(x, (0,2,1))
        return x
    
    #raw_input  = Input(shape=train_df.select("features").first()[0].shape, name='raw')
    raw_input  = Input(shape=input_dim, name='raw')
    
    hist_input = Lambda(get_hist,  name="hist")(raw_input)
    meta_input = Lambda(get_meta,  name="meta")(raw_input)
    band_input = Lambda(get_band,  name="band")(raw_input)
    
    band_emb = Embedding(8, 8)(band_input)
    hist = concatenate([hist_input, band_emb])
    hist = TimeDistributed(Dense(40, activation='relu'))(hist)
    
    rnn = Bidirectional(GRU(size, return_sequences=True))(hist)
    rnn = SpatialDropout1D(0.5)(rnn)
    
    gmp = GlobalMaxPool1D()(rnn)
    gmp = Dropout(0.5)(gmp)
    
    x = concatenate([meta_input, gmp])
    x = Dense(128, activation='relu')(x)
    x = Dense(128, activation='relu')(x)
    x = Dropout(0.5)(x)
    
    output = Dense(15, activation='softmax')(x)
    model = Model(inputs=[raw_input], outputs=output)
    
    return model

# ====================================================
# Data manipulation functions to be called by UDF
# ====================================================

def pad_array(x, sequence_len):
    x = np.pad(x, (sequence_len,0), 'constant', constant_values=(0))
    x= x[len(x)-sequence_len:len(x)]
    return x

def fwd_intervals(x):
    x=np.ediff1d(x, to_begin = [0])
    return x

def bwd_intervals(x):
    x=np.ediff1d(x, to_end = [0])
    return x


# ====================================================
# Write results to Parquet table
# ====================================================

def writeResults(sc, resultDF, vMode, vFormat, vTable):
    
    resultDF.write.mode(vMode).format(vFormat).saveAsTable(vTable)


if __name__ == '__main__':
    
    # ====================================================
    # Set up the log file
    # ====================================================
    
    LogFileName='Program.log'
    #LogFile=datetime.now().strftime(LogFileName + '_%H_%M_%d_%m_%Y.log')
    LogFile='Program.log'    
    logger = logging.getLogger('myapp')
    hdlr = logging.FileHandler(LogFile)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    
    logger.info("LogFile {}".format(LogFile))
    logger.info("Starting...building Spark session and sqlContext") 

    # ====================================================
    # get the runtime parameters
    # ====================================================
    #bTrainModel=sys.argv[1].lower() == 'true'
    
    #if bTrainModel:
    #    logger.info("The model will be trained")
    #else:
    #    logger.info("The model is not trained - this run for vector creation only")
    

    # ====================================================
    # Set up the constants
    # ====================================================
    augment_count = 25
    batch_size = 1000
    batch_size2 = 5000
    optimizer = 'nadam'
    num_models = 1
    use_specz = False
    valid_size = 0.1
    max_epochs = 2

    limit = 1000000
    sequence_len = 256
    print(sequence_len)

    classes = np.array([6, 15, 16, 42, 52, 53, 62, 64, 65, 67, 88, 90, 92, 95, 99], dtype='int32')
    class_names = \
    ['class_6','class_15','class_16','class_42',\
     'class_52','class_53','class_62','class_64',\
     'class_65','class_67','class_88','class_90',\
     'class_92','class_95','class_99']
    class_weight = {6: 1, 15: 2, 16: 1, 42: 1, 52: 1, 53: 1, 62: 1, 64: 2, 65: 1, 67: 1, 88: 1, 90: 1, 92: 1, 95: 1, 99: 1}
    num_classes = len(classes)

    # LSST passbands (nm)  u    g    r    i    z    y      
    passbands = np.array([357, 477, 621, 754, 871, 1004], dtype='float32')

    # ====================================================
    # Sparl SQL Datatype definiions
    # ====================================================

    iType=IntegerType()
    dType=DoubleType()
    fType=FloatType()

    # ====================================================
    # UDF Definitions
    # ====================================================
    
    target_categorical = udf(
        lambda arr:
                [int(i+1 == arr) for i in range(num_classes)],
                ArrayType(iType)
        )

    get_padded_float_vectors = udf(
        lambda arr: pad_array(arr,sequence_len), 
        ArrayType(fType)
    )

    get_padded_int_vectors = udf(
        lambda arr: pad_array(arr,sequence_len), 
        ArrayType(iType)
    )

    toDenseUdf = udf(
        lambda arr: Vectors.dense(arr.toArray()), 
        VectorUDT()
    )

    fwd_udf = udf(
        lambda arr: fwd_intervals(arr), 
        ArrayType(fType)
    )

    bwd_udf = udf(
        lambda arr: bwd_intervals(arr), 
        ArrayType(fType)
    )

    to_vector = udf(lambda a: Vectors.dense(a), VectorUDT())

    
    pgm_start=time.time()
    pgm_startCpu=time.clock()
    
    # ====================================================
    # Set up hive and spark contexts
    # ====================================================
    
    sc = SparkSession.builder.enableHiveSupport().getOrCreate()
    sCtx=sc.sparkContext
    sc.sql("use plasticc")
    
    # ====================================================
    # get the session parameters for the results
    # ====================================================
    appId=sc._jsc.sc().applicationId()
    logger.info(appId)
    
    execMemory = '4g'
    execCores = 1
    driverMemory = '4g'
    driverCores = 1       ## Can only set in cluster mode
    defaultParallel = 20
    deployMode = 'Client'
    numExecutors = 'Dynamic'
    appName = 'Not set'
    
    for i in sorted(sCtx.getConf().getAll()):
        if 'spark.executor.memory' in i:
            # --executor-memory
            logger.info(i)
            execMemory = i[1]
        if 'spark.executor.cores' in i:
            # --executor-cores
            logger.info(i)
            execCores = i[1]
        if 'spark.driver.cores' in i:
            # --driver-cores
            logger.info(i)
            driverCores = i[1]
        if 'spark.default.parallelism' in i:
            logger.info(i)
            defaultParallel = i[1]
        if 'spark.driver.memory' in i:
            # --driver-memory
            logger.info(i)
            driverMemory = i[1]
        if 'spark.submit.deployMode' in i:
            # --deploy-mode
            logger.info(i)
            deployMode = i[1]
        if 'spark.executor.instances' in i:
            # --num-executors
            logger.info(i)
            numExecutors = i[1]
        if 'spark.app.name' in i:
            logger.info(i)
            appName = i[1]    
    
    
    start_elapsed=time.time()
    start_cpu=time.clock()
    
    logger.info("Beginning Fifth model test at {}".format(str(start_elapsed)) )
    
    # ====================================================
    # Get the vectors
    # ====================================================


    
    start=time.time()
    startCpu=time.clock()
    
    trainingVectorsDF=sc.sql("""
    select object_id, target,meta, band, mjd, flux, flux_err, detected, fwd_int, bwd_int,
    source_wavelength, received_wavelength
    from elephas_training_set
    """).persist()

    elapsed_train_df  = time.time() - start
    elapsed_train_dfC = time.clock() - startCpu
    elapsed_samples  = time.time() - start
    elapsed_samplesC = time.clock() - startCpu
    
    logger.info("--- dataframe creation Elapsed - %s seconds - Cpu seconds %s ---" % (elapsed_train_df, elapsed_train_dfC))
    logger.info("--- Samples Elapsed - %s seconds - Cpu seconds %s ---" % (elapsed_samples, elapsed_samplesC))

    wtable = get_wtable(trainingVectorsDF)

    # ====================================================
    # From the final Vector dataframe, create the Python Dictionary
    # in the original format required.
    # ====================================================
    start=time.time()
    startCpu=time.clock()
    
    for i in range(1, num_models+1):
        start=time.time()
        startCpu=time.clock()
        
        trainingVectorsDF = trainingVectorsDF.select(\
            "object_id","target",\
            to_vector("meta").alias("meta"),\
            to_vector(get_padded_int_vectors("band")).alias("band"),\
            to_vector(get_padded_float_vectors("mjd")).alias("mjd"),\
            to_vector(get_padded_float_vectors("flux")).alias("flux"),\
            to_vector(get_padded_float_vectors("flux_err")).alias("flux_err"),\
            to_vector(get_padded_int_vectors("detected")).alias("detect"),\
            to_vector(fwd_udf(get_padded_float_vectors("fwd_int"))).alias("fwd_int"),\
            to_vector(bwd_udf(get_padded_float_vectors("bwd_int"))).alias("bwd_int"),\
            to_vector(get_padded_float_vectors("source_wavelength")).alias("source_wavelength"),\
            to_vector(get_padded_float_vectors("received_wavelength")).alias("received_wavelength")\
            )

        ignore = ['object_id', 'target']
        
        assembler = VectorAssembler(
            inputCols=[x for x in trainingVectorsDF.columns if x not in ignore],
            outputCol='features')
        
        trainingVectorsDF=assembler.transform(trainingVectorsDF)
        
        elapsed_augment=time.time()- start
        elapsed_augmentCpu=time.clock() - startCpu
        
        start=time.time()
        startCpu=time.clock()
        
        weights = [.7, .2, .1]
        seed = 42 # seed=0L  validation_df, 
        train_df, test_df, validation_df = trainingVectorsDF.select( toDenseUdf("features").alias("features"), "target").randomSplit(weights, seed)
        
        train_df=train_df.repartition(400)

        
        elapsed_training_Vectors=time.time()- start
        elapsed_training_VectorsCpu=time.clock() - startCpu
        elapsed_validation_Vectors=time.time()- start
        elapsed_validation_VectorsCpu=time.clock() - startCpu
        
        start=time.time()
        startCpu=time.clock()
        
        input_dim = train_df.select("features").first()[0].shape
        logger.info(f"We have {num_classes} classes and {input_dim[0]} features")
        
        model = get_model(train_df, input_dim)
        model.compile(optimizer=optimizer, loss=mywloss, metrics=['accuracy'])
        adam=optimizers.nadam(lr=0.01)
        opt_conf = optimizers.serialize(adam)
        
        # Initialize SparkML Estimator and set all relevant properties
        estimator = ElephasEstimator()
        estimator.setFeaturesCol("features")             # These two come directly from pyspark,
        estimator.setLabelCol("target")                 # hence the camel case. Sorry :)
        estimator.set_keras_model_config(model.to_yaml())       # Provide serialized Keras model
        estimator.set_categorical_labels(True)
        estimator.set_nb_classes(num_classes)
        estimator.set_num_workers(10)  # We just use one worker here. Feel free to adapt it.
        estimator.set_epochs(2) # was max-epochs
        estimator.set_batch_size(batch_size) # was 128
        estimator.set_verbosity(2) # was 1
        estimator.set_validation_split(0.15)
        estimator.set_optimizer_config(opt_conf)
        estimator.set_mode("synchronous") # Was synchronous
        estimator.set_loss(mywloss) # was("categorical_crossentropy")
        estimator.set_metrics(['accuracy'])

        
        buildModelElapsed=time.time()- start
        buildModelElapseCpu=time.clock() - startCpu
        
        start=time.time()
        startCpu=time.clock()
        
        pipeline = Pipeline(stages=[estimator])
        
        fitted_pipeline = pipeline.fit(train_df)

        trainModelElapsed=time.time()- start 
        trainModelElapseCpu=time.clock() - startCpu
        
        start=time.time()
        startCpu=time.clock()
        
        validation_df=validation_df.repartition(400)
        
        pred = fitted_pipeline.stages[-1]._transform(validation_df, useModel=True)
        
        predictModelElapsed=time.time()- start 
        predictModelElapseCpu=time.clock() - startCpu

        pgm_stop=time.time()- pgm_start
        pgm_stopCpu=time.clock() - pgm_startCpu
        # ====================================================
        # Record results
        # ====================================================
        Results = Row("AppId",
                  "ExecMemory",
                  "ExecCores",
                  "DriverMemory",
                  "DriverCores",
                  "SparkParallelization",
                  "DeployMode",
                  "NumExecutors",
                  "ApplicationName",
                  "DataframeCreationElapsed",
                  "DataframeCreationElapsedCpu",
                  "sampleCreationElapsed",
                  "sampleCreationElapsedCpu",
                  "augmentElapsed",
                  "augmentElapsedCpu",
                  "TrainingVectorCreationElapsed",
                  "TrainingVectorCreationElapsedCpu",
                  "ValidVectCreationElapsed",
                  "ValidVectCreationElapsedCpu", 
                  "BuildModelElapsed", 
                  "BuildModelElapsedCpu", 
                  "TrainModelElapsed", 
                  "TrainModelElapsedCpu", 
                  "PredictModelElapsed", 
                  "PredictModelElapsedCpu", 
                  "total_elapsed",    
                  "total_elapsedCPU")
        result=Results(appId,
                   execMemory,
                   execCores,
                   driverMemory,
                   driverCores,
                   defaultParallel,
                   deployMode,
                   numExecutors,
                   appName,
                   elapsed_train_df,
                   elapsed_train_dfC,
                   elapsed_samples,
                   elapsed_samplesC,
                   elapsed_augment,
                   elapsed_augmentCpu,                   
                   elapsed_training_Vectors,
                   elapsed_training_VectorsCpu,    
                   elapsed_validation_Vectors,
                   elapsed_validation_VectorsCpu,
                   buildModelElapsed, 
                   buildModelElapseCpu,
                   trainModelElapsed, 
                   trainModelElapseCpu,
                   predictModelElapsed,
                   predictModelElapseCpu,                       
                   pgm_stop,    
                   pgm_stopCpu)



        resultsDF=sc.createDataFrame([result])

        MODE='append'
        FORMAT='parquet'
        TABLE='PlasticEndToEndTesting'

        writeResults(sc, resultsDF, MODE, FORMAT, TABLE)
        
    sc.stop()
    logger.info('All finished')
