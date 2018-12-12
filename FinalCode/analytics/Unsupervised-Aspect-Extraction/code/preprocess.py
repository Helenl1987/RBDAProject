from sklearn.feature_extraction.text import CountVectorizer
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
import codecs

def parseSentence(line):
    lmtzr = WordNetLemmatizer()    
    stop = stopwords.words('english')

    text_token = CountVectorizer().build_tokenizer()(line.lower())
    text_rmstop = [i for i in text_token if i not in stop]
    text_stem = [lmtzr.lemmatize(w) for w in text_rmstop]
    return text_stem

def preprocess_train(domain):
    f = codecs.open('../datasets/'+domain+'/train.txt', 'r', 'utf-8')
    out = codecs.open('../preprocessed_data/'+domain+'/train.txt', 'w', 'utf-8')

    for line in f:
        fields = line.split('\t')
        if len(fields) > 6:
            tokens = parseSentence(fields[5])
        else:
            tokens = parseSentence(line)
        if len(tokens) > 0:
            out.write(' '.join(tokens)+'\n')

def preprocess_test(domain):
    # For restaurant domain, only keep sentences with single 
    # aspect label that in {Food, Staff, Ambience}
    #f = codecs.open('../datasets/'+domain+'/test.txt', 'r', 'utf-8')
    #out = codecs.open('../preprocessed_data/'+domain+'/test.txt', 'w', 'utf-8')
    # for line in f:
    #     line = (line.split('\t'))[5]
    #     tokens = parseSentence(line)
    #     if len(tokens) > 0:
    #         out.write(' '.join(tokens)+'\n')
    #     else:
    #         out.write("NULL\n")

    for i in range(200):
        fnum = str(i).zfill(3)
        fname = "/test/part-00"+fnum
        print "preprocessing "+fname+'...'
        f1 = codecs.open('../datasets/'+domain+fname, 'r', 'utf-8')
        out1 = codecs.open('../preprocessed_data/'+domain+fname, 'w', 'utf-8')
        #out2 = codecs.open('../preprocessed_data/'+domain+'/test_label.txt', 'w', 'utf-8')

        # for text, label in zip(f1, f2):
        #     label = label.strip()
        #     if domain == 'restaurant' and label not in ['Food', 'Staff', 'Ambience']:
        #         continue
        #     tokens = parseSentence(text)
        #     if len(tokens) > 0:
        #         out1.write(' '.join(tokens) + '\n')
        #         out2.write(label+'\n')
        for text in f1:
            text = (text.split('\t'))[5]
            tokens = parseSentence(text)
            if len(tokens) > 0:
                out1.write(' '.join(tokens) + '\n')
            else:
                out1.write("NULL\n")


def preprocess(domain):
    print '\t'+domain+' train set ...'
    #preprocess_train(domain)
    print '\t'+domain+' test set ...'
    preprocess_test(domain)

print 'Preprocessing raw review sentences ...'
preprocess('restaurant')
#preprocess('beer')


