import argparse
import logging
import numpy as np
from time import time
import utils as U
from sklearn.metrics import classification_report
import codecs

######### Get hyper-params in order to rebuild the model architecture ###########
# The hyper parameters should be exactly the same as those used for training
parser = argparse.ArgumentParser()
parser.add_argument("-o", "--out-dir", dest="out_dir_path", type=str, metavar='<str>', required=True, help="The path to the output directory")
parser.add_argument("-e", "--embdim", dest="emb_dim", type=int, metavar='<int>', default=200, help="Embeddings dimension (default=200)")
parser.add_argument("-b", "--batch-size", dest="batch_size", type=int, metavar='<int>', default=50, help="Batch size (default=50)")
parser.add_argument("-v", "--vocab-size", dest="vocab_size", type=int, metavar='<int>', default=9000, help="Vocab size. '0' means no limit (default=9000)")
parser.add_argument("-as", "--aspect-size", dest="aspect_size", type=int, metavar='<int>', default=14, help="The number of aspects specified by users (default=14)")
parser.add_argument("--emb", dest="emb_path", type=str, metavar='<str>', help="The path to the word embeddings file")
parser.add_argument("--epochs", dest="epochs", type=int, metavar='<int>', default=15, help="Number of epochs (default=15)")
parser.add_argument("-n", "--neg-size", dest="neg_size", type=int, metavar='<int>', default=20, help="Number of negative instances (default=20)")
parser.add_argument("--maxlen", dest="maxlen", type=int, metavar='<int>', default=0, help="Maximum allowed number of words during training. '0' means no limit (default=0)")
parser.add_argument("--seed", dest="seed", type=int, metavar='<int>', default=1234, help="Random seed (default=1234)")
parser.add_argument("-a", "--algorithm", dest="algorithm", type=str, metavar='<str>', default='adam', help="Optimization algorithm (rmsprop|sgd|adagrad|adadelta|adam|adamax) (default=adam)")
parser.add_argument("--domain", dest="domain", type=str, metavar='<str>', default='restaurant', help="domain of the corpus {restaurant, beer}")
parser.add_argument("--ortho-reg", dest="ortho_reg", type=float, metavar='<float>', default=0.1, help="The weight of orthogonol regularizaiton (default=0.1)")

args = parser.parse_args()
out_dir = args.out_dir_path + '/' + args.domain
out_dir = 'train_tpl_20/' + args.domain
U.print_args(args)

assert args.algorithm in {'rmsprop', 'sgd', 'adagrad', 'adadelta', 'adam', 'adamax'}
assert args.domain in {'restaurant', 'beer'}

from keras.preprocessing import sequence
import reader as dataset

###### Get test data #############
vocab, train_x, test_x, overall_maxlen = dataset.get_data(args.domain, vocab_size=args.vocab_size, maxlen=args.maxlen)

#test_x = sequence.pad_sequences(test_x, maxlen=overall_maxlen)


############# Build model architecture, same as the model used for training #########
from model import create_model
import keras.backend as K
from optimizers import get_optimizer

optimizer = get_optimizer(args)

def max_margin_loss(y_true, y_pred):
    return K.mean(y_pred)
model = create_model(args, overall_maxlen, vocab)

## Load the save model parameters
model.load_weights(out_dir+'/model_param')
model.compile(optimizer=optimizer, loss=max_margin_loss, metrics=[max_margin_loss])



################ Evaluation ####################################

def evaluation(true, predict, domain):
    true_label = []
    predict_label = []

    if domain == 'restaurant':

        for line in predict:
            predict_label.append(line.strip())

        for line in true:
            true_label.append(line.strip())

        print(classification_report(true_label, predict_label, 
            ['Food', 'Staff', 'Ambience', 'Anecdotes', 'Price', 'Miscellaneous'], digits=3))

    else:
        for line in predict:
            label = line.strip()
            if label == 'smell' or label == 'taste':
              label = 'taste+smell'
            predict_label.append(label)

        for line in true:
            label = line.strip()
            if label == 'smell' or label == 'taste':
              label = 'taste+smell'
            true_label.append(label)

        print(classification_report(true_label, predict_label, 
            ['feel', 'taste+smell', 'look', 'overall', 'None'], digits=3))


def prediction(aspect_probs, cluster_map, num, domain):
    label_ids = np.argsort(aspect_probs, axis=1)[:,-1]
    predict_labels = [cluster_map[label_id] for label_id in label_ids]
    aspect_out = codecs.open(out_dir + '/aspects'+str(num).zfill(3), 'w', 'utf-8')
    for label in predict_labels:
        aspect_out.write(label + '\n')
    #evaluation(open(test_labels), predict_labels, domain)


## Create a dictionary that map word index to word 
vocab_inv = {}
for w, ind in vocab.items():
    vocab_inv[ind] = w


test_fn = K.function([model.get_layer('sentence_input').input, K.learning_phase()], 
        [model.get_layer('att_weights').output, model.get_layer('p_t').output])


cluster_map = {0: 'Food', 1: 'Miscellaneous', 2: 'Food', 3: 'Ambience',
           4: 'Staff', 5: 'Staff', 6:'Anecdotes',  7: 'Miscellaneous', 8: 'Miscellaneous', 
           9: 'Food', 10: 'Food', 11: 'Food', 
           12: 'Food', 13: 'Food', 14: 'Food', 15: 'Food', 16: 'Food', 17: 'Miscellaneous', 18: 'Price', 19: 'Ambience'}

## Save attention weights on test sentences into a file 
for i in range(200):
    number = i
    fname = "/test/part-00"+str(i).zfill(3)
    print 'start testing on ' + fname
    att_out = codecs.open(out_dir + '/att_weights'+str(i).zfill(3), 'w', 'utf-8')
    ap_out = codecs.open(out_dir + '/aspect_probs'+str(i).zfill(3), 'w', 'utf-8')

    test_x, mlen = dataset.read_test(args.domain, i, vocab, maxlen=args.maxlen)
    test_x = sequence.pad_sequences(test_x, maxlen=overall_maxlen)
    att_weights, aspect_probs = test_fn([test_x, 0])
    print 'Saving attention weights on test sentences...'
    for c in xrange(len(test_x)):
        att_out.write('----------------------------------------\n')
        att_out.write(str(c) + '\n')

        word_inds = [i for i in test_x[c] if i!=0]
        line_len = len(word_inds)
        weights = att_weights[c]
        weights = weights[(overall_maxlen-line_len):]

        words = [vocab_inv[i] for i in word_inds]
        att_out.write(' '.join(words) + '\n')
        for j in range(len(words)):
            att_out.write(words[j] + ' '+str(round(weights[j], 3)) + '\n')

    print 'Saving aspect probability on test sentences...'
    for prob in aspect_probs:
        sprob = [str(p) for p in prob]
        line = ', '.join(sprob)
        ap_out.write(line + '\n')

    prediction(aspect_probs, cluster_map, number, domain=args.domain)



######################################################
# Uncomment the below part for F scores
######################################################

## cluster_map need to be specified manually according to the top words in each inferred aspect (save in aspect.log)

# map for the pre-trained restaurant model (under pre_trained_model/restaurant)



#print '--- Results on %s domain ---' % (args.domain)
#test_labels = '../preprocessed_data/%s/test_label.txt' % (args.domain)



