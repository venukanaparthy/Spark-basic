#Spam classifier using NaiveBayes from spark MLLIB

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.feature import HashingTF


if __name__ == "__main__":
    sc = SparkContext(appName="Spam NB Analysis")

    #Dataset location
    #https://archive.ics.uci.edu/ml/datasets/SMS+Spam+Collection
    ##data format
    #spam<tab>I am not spam. subscribe to win money
    #ham<tab>Mary had a little lamb 

    inputRdd =  sc.textFile("file:/C:/Projects/2015/spark/learning-spark/files/smsspamcollection/SMSSpamCollection2.txt")

    spamRDD1 = inputRdd.filter(lambda x: x.split("\t")[0] == 'spam')    
    spam = spamRDD1.map(lambda x : x.split("\t")[1])
    #print spam.collect()

    hamRDD1 = inputRdd.filter(lambda x: x.split("\t")[0] == 'ham')
    ham = hamRDD1.map(lambda x : x.split("\t")[1])
    #print ham.collect()
    
    # Create a HashingTF instance to map sms text to vectors of 100 features.
    tf = HashingTF(numFeatures = 100)
    # Each messages is split into words, and each word is mapped to one feature.
    spamFeatures = spam.map(lambda msg: tf.transform(msg.split(" ")))
    hamFeatures = ham.map(lambda msg: tf.transform(msg.split(" ")))
    #print spamFeatures.collect()
    
    # Create LabeledPoint datasets for positive (spam) and negative (ham) examples.
    positiveExamples = spamFeatures.map(lambda features: LabeledPoint(1, features))
    negativeExamples = hamFeatures.map(lambda features: LabeledPoint(0, features))
    training_data = positiveExamples.union(negativeExamples)
    training_data.cache() # Cache data since Logistic Regression is an iterative algorithm.

    #create training, test sets
    trainset, testset = training_data.randomSplit([0.6, 0.4])

    #Fit NaiveBayes
    model = NaiveBayes.train(trainset, 1.0)

    #predict
    predictionLabel = testset.map(lambda x: (model.predict(x.features), x.label))
    #print predictionLabel.collect()

    #model accuracy
    accuracy = 1.0 * predictionLabel.filter(lambda (x, y) : x==y).count()/testset.count()
    print "Model accuracy : {:.2f}".format(accuracy)
    
    sc.stop()
