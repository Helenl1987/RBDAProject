import xgboost as xgb
from sklearn.metrics import accuracy_score,average_precision_score

#monotonic constraints
#feature interaction constraints

dtrain = xgb.DMatrix("/Users/zimoli/Downloads/FINALDF_toronto_wss/train_ls/train_t_ls")

dtest = xgb.DMatrix("/Users/zimoli/Downloads/FINALDF_toronto_wss/test_ls/test_t_ls")

param = {'max_depth':6, 'eta':0.2, 'silent':1, 'objective':'multi:softmax', 'num_class':21}

watchlist = [(dtest, 'eval'), (dtrain, 'train')]

num_round = 2

#switch multi to binary in evaluation
def trans2bi(num):
	if num >= 10:
		return 1.0
	else:
		return 0.0

def evalerror(preds, dtrain):
    labels = dtrain.get_label()
    blabels = [trans2bi(val) for val in labels]
    bpreds = [trans2bi(val) for val in preds]
    return 'my-accuracy', accuracy_score(blabels, bpreds)

def evalap(preds, dtrain):
    labels = dtrain.get_label()
    blabels = [trans2bi(val) for val in labels]
    bpreds = [trans2bi(val) for val in preds]
    return 'my-ap', average_precision_score(blabels, bpreds)

bst = xgb.train(param, dtrain, num_round, watchlist, feval=evalap)

preds = bst.predict(dtest)


