

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

import os
import math
import copy
import random
import time
import sys

from pyspark import SparkConf,SparkContext
from pyspark.sql import Row, SQLContext, SparkSession
from pyspark.sql.functions import monotonically_increasing_id, lit, col
from pyspark.sql.types import *
from pyspark.ml.linalg import Vectors


from keras.layers import *
from keras.models import Model, load_model
from keras.optimizers import Adam, Nadam, SGD
from keras.callbacks import EarlyStopping, ModelCheckpoint, TensorBoard
from keras.utils import to_categorical
from keras.preprocessing.sequence import pad_sequences

from sklearn.model_selection import train_test_split, KFold
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import normalize
from sklearn.metrics import confusion_matrix

import tensorflow as tf


# ====================================================
# Logging
# ====================================================

from datetime import datetime

import logging



def append_data(list_x, list_y = None):
    X = {}
    for k in list_x[0].keys():

        list = [x[k] for x in list_x]
        X[k] = np.concatenate(list)

    if list_y is None:
        return X
    else:
        return X, np.concatenate(list_y)

def get_wtable(df):
    # from the original pandas dataframe all_y = np.array(df['target'], dtype = 'int32')
    all_y = np.array(df.select('target').collect(), dtype = 'int32')

    y_count = np.unique(all_y, return_counts=True)[1]

    wtable = np.ones(len(classes))

    for i in range(0, y_count.shape[0]):
        wtable[i] = y_count[i] / all_y.shape[0]

    return wtable

def get_keras_data(itemslist):

    keys = itemslist[0].keys()
    X = {
            'id': np.array([i['id'] for i in itemslist], dtype='int32'),
            'meta': np.array([i['meta'] for i in itemslist]),
            'band': pad_sequences([i['band'] for i in itemslist], maxlen=sequence_len, dtype='int32'),
            'hist': pad_sequences([i['hist'] for i in itemslist], maxlen=sequence_len, dtype='float32'),
        }

    Y = to_categorical([i['target'] for i in itemslist], num_classes=len(classes))

    X['hist'][:,:,0] = 0 # remove abs time
#    X['hist'][:,:,1] = 0 # remove flux
#    X['hist'][:,:,2] = 0 # remove flux err
    X['hist'][:,:,3] = 0 # remove detected flag
#    X['hist'][:,:,4] = 0 # remove fwd intervals
#    X['hist'][:,:,5] = 0 # remove bwd intervals
#    X['hist'][:,:,6] = 0 # remove source wavelength
    X['hist'][:,:,7] = 0 # remove received wavelength

    return X, Y

def set_intervals(sample):

    hist = sample['hist']
    band = sample['band']

    hist[:,4] = np.ediff1d(hist[:,0], to_begin = [0])
    hist[:,5] = np.ediff1d(hist[:,0], to_end = [0])


def copy_sample(s, augmentate=True):
    c = copy.deepcopy(s)

    if not augmentate:
        return c

    band = []
    hist = []

    drop_rate = 0.3

    # drop some records
    for k in range(s['band'].shape[0]):
        if random.uniform(0, 1) >= drop_rate:
            band.append(s['band'][k])
            hist.append(s['hist'][k])

    c['hist'] = np.array(hist, dtype='float32')
    c['band'] = np.array(band, dtype='int32')

    set_intervals(c)
            
    new_z = random.normalvariate(c['meta'][5], c['meta'][6] / 1.5)
    new_z = max(new_z, 0)
    new_z = min(new_z, 5)

    dt = (1 + c['meta'][5]) / (1 + new_z)
    c['meta'][5] = new_z

    # augmentation for flux
    c['hist'][:,1] = np.random.normal(c['hist'][:,1], c['hist'][:,2] / 1.5)

    # multiply time intervals and wavelength to apply augmentation for red shift
    c['hist'][:,0] *= dt
    c['hist'][:,4] *= dt
    c['hist'][:,5] *= dt
    c['hist'][:,6] *= dt

    return c

def normalize_counts(samples, wtable, augmentate):
    maxpr = np.max(wtable)
    counts = maxpr / wtable

    res = []
    index = 0
    for s in samples:

        index += 1
        print('Normalizing {0}/{1}   '.format(index, len(samples)), end='\r')
        logger.info('Normalizing {0}/{1}   '.format(index, len(samples)))
        res.append(s)
        count = int(3 * counts[s['target']]) - 1

        for i in range(0, count):
            res.append(copy_sample(s, augmentate))

    print()

    return res

def augmentate(samples, gl_count, exgl_count):

    res = []
    index = 0
    for s in samples:

        index += 1
        
        if index % 1000 == 0:
            print('Augmenting {0}/{1}   '.format(index, len(samples)), end='\r')
            logger.info('Augmenting {0}/{1}   '.format(index, len(samples)))
        count = gl_count if (s['meta'][8] == 0) else exgl_count

        for i in range(0, count):
            res.append(copy_sample(s))

    print()
    return res

def get_data(raw_vectors_df, extragalactic=None, use_specz=False):

    samples = []
    list_objects = map(lambda row: row.asDict(), raw_vectorsDF.collect())
    object_vectors = {object['object_id']: object for object in list_objects}


    for key in object_vectors.keys():

        i=object_vectors.get(key)

        id=i.get('object_id')

        sample = {}
        sample['id'] = int(id)

        # 'object_id', 'target', 'meta', 'specz', 'band', 'hist'
        sample['target'] = np.where(classes == int(i.get('target')))[0][0] # positional index of the classes array

        meta=np.array(i.get('meta'), dtype='float32')

        sample['meta'] = np.zeros(10, dtype = 'float32')

            #sample['meta'][4] = meta['ddf']					from meta column array meta[0]
            #sample['meta'][5] = meta['hostgal_photoz']			from meta column array meta[2]
            #sample['meta'][6] = meta['hostgal_photoz_err']		from meta column array meta[3]
            #sample['meta'][7] = meta['mwebv']					from meta column array meta[4]
            #sample['meta'][8] = float(meta['hostgal_photoz']) > 0  returns True or false

            #sample['specz'] = float(meta['hostgal_specz'])		from meta column array meta[1]


        sample['meta'][4] = meta[0]
        sample['meta'][5] = meta[2]
        sample['meta'][6] = meta[3]
        sample['meta'][7] = meta[4]
        sample['meta'][8] = float(meta[2]) > 0

        sample['specz'] = float(meta[1])    

        if use_specz:
            sample['meta'][5] = float(meta['hostgal_specz'])
            sample['meta'][6] = 0.0

        z = float(sample['meta'][5])
        
        ## Note! refer note regarding the table creation in the notebook "ThirdModelTest.ipynb"
        ## as the table using in this example does not cast a collect_list to an array.
        ## Therefore, a reshape is not necessary!

        j=i.get('hist')
        mjd=np.array(j[0][0], dtype='float32')
        #r,c=mjd.shape
        
        #mjd.reshape(c,)
       
        band=np.array(i.get('band') , dtype='int32')
        #band=np.array(  j[0][1], dtype='int32') #.reshape(c,) # passband
        flux=np.array( j[0][1], dtype='float32') #.reshape(c,) # flux
        flux_err=np.array( j[0][2], dtype='float32') #.reshape(c,) # flux_err
        detected=np.array( j[0][3], dtype='int32') #.reshape(c,) # Detected

        mjd -= mjd[0]
        mjd /= 100 # Earth time shift in day*100
        mjd /= (z + 1) # Object time shift in day*100


        received_wavelength = passbands[band] # Earth wavelength in nm
        received_freq = 300000 / received_wavelength # Earth frequency in THz
        source_wavelength = received_wavelength / (z + 1) # Object wavelength in nm


        sample['band'] = band + 1

        sample['hist'] = np.zeros((flux.shape[0], 8), dtype='float32')
        sample['hist'][:,0] = mjd
        sample['hist'][:,1] = flux
        sample['hist'][:,2] = flux_err
        sample['hist'][:,3] = detected

        sample['hist'][:,6] = (source_wavelength/1000)
        sample['hist'][:,7] = (received_wavelength/1000)

        set_intervals(sample)

        flux_max = np.max(flux)
        flux_min = np.min(flux)
        flux_pow = math.log2(flux_max - flux_min)
        sample['hist'][:,1] /= math.pow(2, flux_pow)
        sample['hist'][:,2] /= math.pow(2, flux_pow)
        sample['meta'][9] = flux_pow / 10

        samples.append(sample)

        if len(samples) % 1000 == 0:
            print('Converting data {0}'.format(len(samples)), end='\r')

        if len(samples) >= limit:
            break



    print()
    return samples

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

def train_model(i, samples_train, samples_valid, bTrainModel):
    start_augment=time.time()
    start_augmentCpu=time.clock()
    
    samples_train += augmentate(samples_train, augment_count, augment_count)
    
    elapsed_augment=time.time() - start_augment
    elapsed_augmentCpu=time.clock() - start_augmentCpu

    patience = 1000000 // len(samples_train) + 5

    start_trainingVectors=time.time()
    start_trainingVectorsCpu=time.clock()

    train_x, train_y = get_keras_data(samples_train)

    elapsed_training_Vectors=time.time() - start_trainingVectors
    elapsed_training_VectorsCpu=time.clock() - start_trainingVectorsCpu

    
    del samples_train
    
    start_validationVectors=time.time()
    start_validationVectorsCpu=time.clock()

    valid_x, valid_y = get_keras_data(samples_valid)
    del samples_valid
    
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
    
    logger.info("Beginning Third pivot model test at {}".format(str(start_elapsed)) )
    


    # ====================================================
    # create the dataframes
    # ====================================================


    logger.info('Loading train data from hive...')
    
    start_train_df=time.time()
    start_train_dfC=time.clock()
    
    raw_vectorsDF=sc.sql("select * from training_raw_vectors_unpadded_no_calcs")

    elapsed_train_df=time.time() - start_train_df
    elapsed_train_dfC=time.clock() - start_train_dfC

    wtable = get_wtable(raw_vectorsDF)
    
    # ====================================================
    # create samples
    # ====================================================

    start_samples=time.time()
    start_samplesC=time.clock()

    samples =  get_data(raw_vectorsDF,\
                        extragalactic=None, use_specz=use_specz)

    elapsed_samples=time.time() - start_samples
    elapsed_samplesC=time.clock() - start_samplesC
    
    for i in range(1, num_models+1):

        samples_train, samples_valid = train_test_split(samples, test_size=valid_size, random_state=42*i)
        
        elapsed_augment,elapsed_augmentCpu,\
        elapsed_training_Vectors,elapsed_training_VectorsCpu,\
        elapsed_validation_Vectors,elapsed_validation_VectorsCpu,\
        buildModelElapsed, buildModelElapseCpu,\
        trainModelElapsed, trainModelElapseCpu,\
        predictModelElapsed,predictModelElapseCpu=train_model(i, samples_train, samples_valid, bTrainModel)

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
