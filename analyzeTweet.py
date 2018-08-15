import json
import pandas as pd
import matplotlib.pyplot as plt
import re


def word_in_text(word, text):
    word = word.lower()
    text = text.lower()
    #print(text)
    match = re.search(word, text)
    if match:
        return True
    return False

tweets_data_path = 'twitter_data.txt'

tweets_data = []

tweets_file = open(tweets_data_path, "r")

for line in tweets_file:
    try:
        tweet = json.loads(line)
        tweets_data.append(tweet)
    except:
        continue

#print(tweets_data[3]['lang'])
tweets = pd.DataFrame()

#x = []
#for tweet in tweets_data:
#    x.append(tweet['lang']) 

#print(tweets_data['lang'])

#tweets = pd.DataFrame(x,columns=['lang'])

#tweets['text'] = map(lambda tweet: tweet['text'], tweets_data)
tweets['lang'] = list(map(lambda tweet: tweet['lang'], tweets_data))
tweets['text'] = list(map(lambda tweet: tweet['text'], tweets_data))
tweets['country'] = list(map(lambda tweet: tweet['place']['country'] if tweet['place'] != None else None, tweets_data))

#print(tweets['country'])

tweets_by_lang = tweets['lang'].value_counts()
#print(list(tweets['lang']))

#for i in tweets['lang']:
#    print(list(i))

#fig, ax = plt.subplots()
#ax.tick_params(axis='x', labelsize=15)
#ax.tick_params(axis='y', labelsize=10)
#ax.set_xlabel('Languages', fontsize=15)
#ax.set_ylabel('Number of tweets' , fontsize=15)
#ax.set_title('Top 5 languages', fontsize=15, fontweight='bold')
#tweets_by_lang[:5].plot(ax=ax, kind='bar', color='red')

tweets['python'] = tweets['text'].apply(lambda tweet: word_in_text('python', tweet))
tweets['javascript'] = tweets['text'].apply(lambda tweet: word_in_text('javascript', tweet))
tweets['ruby'] = tweets['text'].apply(lambda tweet: word_in_text('ruby', tweet))

tweets['programming'] = tweets['text'].apply(lambda tweet: word_in_text('programming', tweet))
tweets['tutorial'] = tweets['text'].apply(lambda tweet: word_in_text('tutorial', tweet))

tweets['relevant'] = tweets['text'].apply(lambda tweet: word_in_text('programming', tweet) or word_in_text('tutorial', tweet))

print(tweets['python'].value_counts()[True])
print(tweets['javascript'].value_counts()[True])
print(tweets['ruby'].value_counts()[True])

print(tweets[tweets['relevant'] == True]['python'].value_counts()[True])
print(tweets[tweets['relevant'] == True]['javascript'].value_counts()[True])
print(tweets[tweets['relevant'] == True]['ruby'].value_counts()[True])

prg_langs = ['python', 'javascript', 'ruby']
#tweets_by_prg_lang = [tweets['python'].value_counts()[True], tweets['javascript'].value_counts()[True], tweets['ruby'].value_counts()[True]]
tweets_by_prg_lang = [tweets[tweets['relevant'] == True]['python'].value_counts()[True], 
                      tweets[tweets['relevant'] == True]['javascript'].value_counts()[True], 
                      tweets[tweets['relevant'] == True]['ruby'].value_counts()[True]]

x_pos = list(range(len(prg_langs)))
width = 0.8
fig, ax = plt.subplots()
plt.bar(x_pos, tweets_by_prg_lang, width, alpha=1, color='g')

ax.set_ylabel('Number of tweets', fontsize=15)
ax.set_title('Ranking: python vs. javascript vs. ruby (Raw data)', fontsize=10, fontweight='bold')
ax.set_xticks([p + 0.4 * width for p in x_pos])
ax.set_xticklabels(prg_langs)
plt.grid()

plt.show()
