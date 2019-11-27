# ====================================================
# This is the fifthPivotModel running against the SQL created fully padded, fully calculated table
# ====================================================


import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

import os
import sys
import math
import copy
import random

import time

from pyspark import SparkConf,SparkContext
from pyspark.sql import Row, SQLContext, SparkSession
from pyspark.sql.types import *

from pyspark.ml.feature import StringIndexer, StandardScaler,VectorAssembler,OneHotEncoder
from pyspark.ml import Pipeline

from pyspark.sql.functions import udf, col, array, lit
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

# ====================================================
# Functions for UDFs
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
# Get the keras Data dictionary
# ====================================================
def get_keras_data(sc,trainingVectorsDF, sequence_len, num_classes, recCount=0):

    if recCount == 0:
        r=trainingVectorsDF.count()
    else:
        r=recCount
            
    
    idArr=np.array(trainingVectorsDF.select('object_id').collect(), dtype='int32').reshape(r,)
    #idArr.reshape(r,)
    meta_len=10
    
    metaArr=np.array(trainingVectorsDF.select('meta').collect(), dtype='float32').reshape(r,meta_len)
    bandArr= np.array(trainingVectorsDF.select('band').collect() , dtype='int32').reshape(r,sequence_len)

    histArray=np.zeros((r,sequence_len,8), dtype='float32') 
    # this will work brilliantly as get_keras_data sets three columns to zeros anyway
    
    #mjd=np.array(trainingVectorsDF.select('mjd').collect(), dtype='float32').reshape(r,sequence_len)
    flux=np.array(trainingVectorsDF.select('hist.flux').collect(), dtype='float32').reshape(r,sequence_len)
    flux_err=np.array(trainingVectorsDF.select('hist.flux_err').collect(), dtype='float32').reshape(r,sequence_len)
    #detect=np.array(trainingVectorsDF.select('detected').collect(), dtype='float32').reshape(r,sequence_len)
    fwd_int=np.array(trainingVectorsDF.select('hist.deltaMjd').collect(), dtype='float32').reshape(r,sequence_len)
    bwd_int=np.array(trainingVectorsDF.select('hist.rval').collect(), dtype='float32').reshape(r,sequence_len)
    source_wavelength=np.array(trainingVectorsDF.select('hist.source_wavelength').collect(), dtype='float32').reshape(r,sequence_len)
    #received_wavelength=np.array(trainingVectorsDF.select('received_wavelength').collect(), dtype='float32').reshape(r,sequence_len)
    
    #as per the baseline program, we remove the abs time, detected and receoved_wavelength data

    #histArray[:,:,0]=mjd
    histArray[:,:,1]=flux
    histArray[:,:,2]=flux_err
    #histArray[:,:,3]=detect
    histArray[:,:,4]=fwd_int
    histArray[:,:,5]=bwd_int
    histArray[:,:,6]=source_wavelength
    #histArray[:,:,7]=received_wavelength

    # Create the final vector dictionary
    X = {
            'id': idArr,
            'meta': metaArr,
            'band': bandArr,
            'hist': histArray
        }
    # and the encoded target vector
    
    Y = np.array(trainingVectorsDF.select(target_categorical('target')).collect(), dtype='int32').reshape(r, num_classes)  
    
    return X, Y

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

def get_model(X, Y, size=80):

    hist_input = Input(shape=X['hist'][0].shape, name='hist')
    meta_input = Input(shape=X['meta'][0].shape, name='meta')
    band_input = Input(shape=X['band'][0].shape, name='band')

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

    model = Model(inputs=[hist_input, meta_input, band_input], outputs=output)

    return model


def train_model(i, trainingVectorsDF, bTrainModel):
    start_augment=time.time()
    start_augmentCpu=time.clock()
    
    #samples_train += augmentate(samples_train, augment_count, augment_count)
    
    elapsed_augment=time.time() - start_augment
    elapsed_augmentCpu=time.clock() - start_augmentCpu

    
    # ====================================================
    # Set up the training, test and valiation splits
    # ====================================================

    start=time.time()
    startCpu=time.clock()

    weights = [.8, .1, .1]
    seed = 42 # seed=0L
    train_df, validation_df, test_df = trainingVectorsDF.randomSplit(weights, seed)

    samples_train=train_df.count()
    valid_train=validation_df.count()
    patience = 1000000 // samples_train + 5

    splitElapsed = time.time() - start
    splitElapsedCPU = time.clock() - startCpu

    logger.info("--- Test and validation splits on the dataframe Elapsed - %s seconds - Cpu seconds %s ---" \
                % (splitElapsed, splitElapsedCPU))    

    start_trainingVectors=time.time()
    start_trainingVectorsCpu=time.clock()

    train_x, train_y = get_keras_data(sc,train_df, sequence_len, num_classes, samples_train)

    elapsed_training_Vectors=time.time() - start_trainingVectors
    elapsed_training_VectorsCpu=time.clock() - start_trainingVectorsCpu

    
    start_validationVectors=time.time()
    start_validationVectorsCpu=time.clock()

    valid_x, valid_y = get_keras_data(sc,validation_df, sequence_len, num_classes, valid_train)
    
    elapsed_validation_Vectors=time.time() - start_validationVectors
    elapsed_validation_VectorsCpu=time.clock() - start_validationVectorsCpu
    
    start=time.time()
    startC=time.clock()
    model = get_model(train_x, train_y)

    if i == 1: model.summary()
    model.compile(optimizer=optimizer, loss=mywloss, metrics=['accuracy'])
    buildModelElapsed=time.time() - start
    buildModelElapseCpu=time.clock() - startC

    start=time.time()
    startC=time.clock()
    
    if bTrainModel:
        print('Training model {0} of {1}, Patience: {2}'.format(i, num_models, patience))
        filename = 'model_{0:03d}.hdf5'.format(i)
        callbacks = [EarlyStopping(patience=patience, verbose=1), ModelCheckpoint(filename, save_best_only=True)]
        model.fit(train_x, train_y, validation_data=(valid_x, valid_y), epochs=max_epochs, batch_size=batch_size, callbacks=callbacks, verbose=2)

    trainModelElapsed=time.time() - start
    trainModelElapseCpu=time.clock() - startC

    start=time.time()
    startC=time.clock()
    
    if bTrainModel:
        model = load_model(filename, custom_objects={'mywloss': mywloss})
        preds = model.predict(valid_x, batch_size=batch_size2)
        loss = multi_weighted_logloss(valid_y, preds, wtable)
        acc = accuracy_score(np.argmax(valid_y, axis=1), np.argmax(preds,axis=1))
        logger.info('MW Loss: {0:.4f}, Accuracy: {1:.4f}'.format(loss, acc))

    predictModelElapsed=time.time() - start
    predictModelElapseCpu=time.clock() - startC
    
    
    
    return  elapsed_augment,elapsed_augmentCpu,\
    elapsed_training_Vectors,elapsed_training_VectorsCpu,\
    elapsed_validation_Vectors,elapsed_validation_VectorsCpu,\
    buildModelElapsed, buildModelElapseCpu,\
    trainModelElapsed, trainModelElapseCpu,\
    predictModelElapsed,predictModelElapseCpu    
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
    bTrainModel=sys.argv[1].lower() == 'true'
    
    if bTrainModel:
        logger.info("The model will be trained")
    else:
        logger.info("The model is not trained - this run for vector creation only")
    

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

    classes = np.array([6, 15, 16, 42, 52, 53, 62, 64, 65, 67, 88, 90, 92, 95, 99], dtype='int32')
    class_names = \
    ['class_6','class_15','class_16','class_42',\
     'class_52','class_53','class_62','class_64',\
     'class_65','class_67','class_88','class_90',\
     'class_92','class_95','class_99']
    class_weight = {6: 1, 15: 2, 16: 1, 42: 1, 52: 1, 53: 1, 62: 1, 64: 2, 65: 1, 67: 1, 88: 1, 90: 1, 92: 1, 95: 1, 99: 1}

    # LSST passbands (nm)  u    g    r    i    z    y      
    passbands = np.array([357, 477, 621, 754, 871, 1004], dtype='float32')
    num_classes = len(classes)

    
    pgm_start=time.time()
    pgm_startCpu=time.clock()
    
    iType=IntegerType()
    dType=DoubleType()
    fType=FloatType()

    # ====================================================
    # UDF definitions
    # ====================================================
    target_categorical = udf(
        lambda arr:
            [int(i+1 == arr) for i in range(num_classes)], 
            ArrayType(iType)       
    )

    get_padded_float_vectors = udf(
        lambda arr: pad_array(arr).tolist(), 
        ArrayType(fType)
    )

    get_padded_int_vectors = udf(
        lambda arr: pad_array(arr).tolist(), 
        ArrayType(iType)
    )

    toDenseUdf = udf(
        lambda arr: Vectors.dense(arr.toArray()), 
        VectorUDT()
    )

    fwd_udf = udf(
        lambda arr: fwd_intervals(arr).tolist(), 
        ArrayType(fType)
    )

    bwd_udf = udf(
        lambda arr: bwd_intervals(arr).tolist(), 
        ArrayType(fType)
    )

    to_vector = udf(lambda a: Vectors.dense(a), VectorUDT())
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
    
    
    vectorTable="training_set_augmented_vectors"
    trainingVectorsDF=sc.sql("select * from {}".format(vectorTable)).persist()

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

        elapsed_augment,elapsed_augmentCpu,\
        elapsed_training_Vectors,elapsed_training_VectorsCpu,\
        elapsed_validation_Vectors,elapsed_validation_VectorsCpu,\
        buildModelElapsed, buildModelElapseCpu,\
        trainModelElapsed, trainModelElapseCpu,\
        predictModelElapsed,predictModelElapseCpu=train_model(i, trainingVectorsDF, bTrainModel)

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
        logger.info("writing results to {}".format(str(TABLE)) )
    sc.stop()
    logger.info('All finished')
