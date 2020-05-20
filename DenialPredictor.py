from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import roc_curve, auc, accuracy_score, f1_score, precision_score, recall_score

class DenialPredictor():
    
    def __init__(self, corpus, labels, clf="nb"):
        self.corpus = corpus
        self.labels = labels
        self.vectorizer = CountVectorizer()
        self.transformer = TfidfTransformer()
        
        if clf == "nb":
            self.classifier = MultinomialNB()
        elif clf =="svm":
            self.classifier = SGDClassifier(loss='log', penalty='l2',
                                            alpha=1e-3, random_state=123,
                                            max_iter=5, tol=None)
        else:
            raise "Invalid argument for classifier choice."
            
    def fit_model(self, X_train, y_train):
        p = Pipeline([
                ('vect', self.vectorizer),
                ('tfidf', self.transformer),
                ('clf', self.classifier),
        ])
        self.model = p.fit(X_train, y_train)
    
    def _vectorize(self):
        return self.vectorizer.fit_transform(self.corpus)
    
    def train_test_split(self, split=0.3):
        #X = self._vectorize()
        return train_test_split(self.corpus, self.labels, random_state=123, test_size=split)
    
    def predict(self, X):
        return self.model.predict(X)
    
    def calc_metrics(self, X, y_truth):
        y_predict = self.predict(X)
        a = accuracy_score(y_truth, y_predict)
        p = precision_score(y_truth, y_predict)
        r = recall_score(y_truth, y_predict)
        f = f1_score(y_truth, y_predict)
#        print("Performance metrics: ")
#        print(f"\t-Accuracy: {a:.3f},\n\t-Precision: {p:.3f}, \n\t-Recall: {r:.3f},\n\t-F1: {f:.3f}")
        print(f"Accuracy: {a:.3f},\nPrecision: {p:.3f}, \nRecall: {r:.3f},\nF1: {f:.3f}")