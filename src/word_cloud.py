#
# step 1: import data

import pandas as pd
pd.set_option('display.max_colwidth', 100)

# read the comment data
df = pd.read_csv('comments.csv')

# show data structure
df.project.value_counts(ascending=True, normalize=True)*100

# count the total number of words
total_count = 0
for i in range(df.shape[0]):
    total_count += len(df.comment[i])

print("the total number of words :", total_count)
print()

# count the max number of words
total_count_list = []
for i in range(df.shape[0]):
    total_count_list.append(len(df.comment[i]))

print("the max number of words :", max(total_count_list))


# 
# step 2: data preprocessing

import re
import random

# only chinese words, english words, punctuations, and numbers are saved
pattern = r'[a-zA-Z0-9\u4e00-\u9fa5，。！：；“”‘’（）《》、·【】《》—-]+'

# update comments
df.comment = df.comment.apply(lambda x: ''.join(re.findall(pattern, x)))

# double check data
# list(df.comment)[:5]

# generate a random number
i = random.randint(1, df.shape[0])

# show data sample
print(df.comment[i])

# split comment by punctuations
result = re.split(r'[，。！、]', df.comment[i])

# filter out None
result = list(filter(None, result))

# double check data
# result

# split all comments by punctuations
df['comment_list'] = df.comment.apply(lambda x: re.split(r'[，。！、]', x))

# filter out None
df.comment_list = df.comment_list.apply(lambda x: list(filter(None, x)))

# double check data
# df.head()

# create a new dataframe

# use explode(), this step is important!!!
# let each element in "comment_list" to be a row
df_exploded = df[['city', 'project', 'comment_list']].explode('comment_list', ignore_index=True)

# rename the column
df_exploded.rename(columns={'comment_list': 'comment'}, inplace=True)

# double check data
# df_exploded.shape
# df_exploded.head()

# show data structure
df_exploded.project.value_counts(ascending=True, normalize=True)*100


#
# step 3: data labelling

from sklearn.model_selection import train_test_split

# split data into training and testing sets
df_train, df_test = train_test_split(df_exploded, test_size=0.9, random_state=42)

# reset index
df_train.reset_index(drop=True, inplace=True)
df_test.reset_index(drop=True, inplace=True)

# download the training dataset
df_train.to_csv('training_set.csv', encoding='utf_8_sig')

# manually labelling...
# 0-neutral; 1-positive; 2-negative;

# import labelled data
df_train_labelled = pd.read_csv('training_set_labelled.csv')[:1000]
df_train_labelled.head()


#
# step 4: convert comments to embeddings -- running time is too long!!!

# pip install sentence-transformers

from sentence_transformers import SentenceTransformer

# use sentence-transformers to convert comments into embeddings
# load pre-trained model for Chinese text
model = SentenceTransformer('./models/paraphrase-multilingual-MiniLM-L12-v2')

# convert the comments into embeddings
train_embeddings = model.encode(df_train_labelled.comment.to_list(), convert_to_tensor=True)
test_embeddings = model.encode(df_test.comment.to_list(), convert_to_tensor=True)


#
# step 5: train a classifier

from sklearn.svm import SVC
import xgboost as xgb

# svm
# classifier = SVC(kernel='linear')  # 'linear', 'rbf', 'poly'
# classifier.fit(train_embeddings.cpu().numpy(), df_train_labelled.label)

# xgboost
classifier = xgb.XGBClassifier()
classifier.fit(train_embeddings.cpu().numpy(), df_train_labelled.label)

# predict new comments
test_labels = classifier.predict(test_embeddings.cpu().numpy())

# display predictions
for comment, label in zip(df_test.comment.to_list()[:10], test_labels[:10]):
    print(f"Comment: {comment} -> Predicted Label: {label}")

# add labels for test comments
df_test_labelled = df_test
df_test_labelled['label'] = test_labels

# concat two dataframe 
df_labelled = pd.concat([df_train_labelled, df_test_labelled], axis=0)
# df_labelled.head()
# df_labelled.shape

# get positive, negative comments
df_positive = df_labelled[df_labelled.label == 1]
df_negative = df_labelled[df_labelled.label == 2]

# double check data
print("training data structure:")
print(df_train_labelled.label.value_counts(ascending=True, normalize=True))
print()
print("overall data structure:")
print(df_labelled.label.value_counts(ascending=True, normalize=True))


#
# step 6: generate word clouds

from wordcloud import WordCloud
import matplotlib.pyplot as plt
import jieba

# define a function for positive word clouds
def positive_word_cloud(data, stop_words): 
    
    # combine all comments into a single string
    text = " ".join(data)

    # remove punctuations
    #text = re.sub(r'[^\w\s]', '', text)

    # use jieba to segment the text
    segmented_text = " ".join(jieba.cut(text))

    # generate the word cloud
    wordcloud = WordCloud(
        font_path='msyh.ttc',
        stopwords=stop_words,
        background_color='white',
        colormap='PuRd',
        width=900,
        height=500
    ).generate(segmented_text)

    # display the word cloud
    plt.figure(figsize=(10, 8))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.show()

# define stop words
stop_words = {'hello'}

# positive word clouds
for i in list(set(df_positive.project)):
    print()
    print(i)
    print()
    positive_word_cloud(df_positive[df_positive.project == i].comment.to_list(), stop_words)
