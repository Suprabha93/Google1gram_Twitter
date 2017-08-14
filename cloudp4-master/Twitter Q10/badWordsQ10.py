#!/usr/bin/env python 
# Python Spark Code for Twitter Problem #6
from __future__ import print_function
from __future__ import division
import json
from pyspark import SparkContext 
sc = SparkContext(appName="badWords") 
twitter = sc.textFile("hdfs:///data/twitter/*")
def hour_good(s):
  word_count = 0
  words = s["text"].lower()
  words_tweet=words.split(' ')
  time_at=s['created_at']
  tweet_hour=time_at.split(' ')[3].split(':')[0]
  for word in words_tweet:
    word_count +=1
  return (int(tweet_hour),word_count)
def hour_bad(s): 
  word_count=0 
  bad_count = 0
  badwords=['alcoholic', 'amateur', 'analphabet', 'anarchist', 'ape', 'arse', 'arselicker', 'ass', 'ass master', 'ass-kisser', 'ass-nugget', 'ass-wipe', 'asshole', 'baby', 'backwoodsman', 'balls', 'bandit', 'barbar', 'bastard', 'bastard', 'beavis', 'beginner', 'biest', 'bitch', 'blubber gut', 'bogeyman', 'booby', 'boozer', 'bozo', 'brain-fart', 'brainless', 'brainy', 'brontosaurus', 'brownie', 'bugger', 'bugger', 'bulloks', 'bum', 'bum-fucker', 'butt', 'buttfucker', 'butthead', 'callboy', 'callgirl', 'camel', 'cannibal', 'cave man', 'chaavanist', 'chaot', 'chauvi', 'cheater', 'chicken', 'children fucker', 'clit', 'clown', 'cock', 'cock master', 'cock up', 'cockboy', 'cockfucker', 'cockroach', 'coky', 'con merchant', 'con-man', 'country bumpkin', 'cow', 'creep', 'creep', 'cretin', 'criminal', 'cunt', 'cunt sucker', 'daywalker', 'deathlord', 'derr brain', 'desperado', 'devil', 'dickhead', 'dinosaur', 'disguesting packet', 'diz brain', 'do-do', 'dog', 'dog , dirty', 'dogshit', 'donkey', 'drakula', 'dreamer', 'drinker', 'drunkard', 'dufus', 'dulles', 'dumbo', 'dummy', 'dumpy', 'egoist', 'eunuch', 'exhibitionist', 'fake', 'fanny', 'farmer', 'fart', 'fart , shitty', 'fatso', 'fellow', 'fibber', 'fish', 'fixer', 'flake', 'flash harry', 'freak', 'frog', 'fuck', 'fuck face', 'fuck head', 'fuck noggin', 'fucker', 'gangster', 'ghost', 'goose', 'gorilla', 'grouch', 'grumpy', 'head, fat', 'hell dog', 'hillbilly', 'hippie', 'homo', 'homosexual', 'hooligan', 'horse fucker', 'idiot', 'ignoramus', 'jack-ass', 'jerk', 'joker', 'junkey', 'killer', 'lard face', 'latchkey child', 'learner', 'liar', 'looser', 'lucky', 'lumpy', 'luzifer', 'macho', 'macker', 'man, old', 'minx', 'missing link', 'monkey', 'monster', 'motherfucker', 'mucky pub', 'mutant', 'neanderthal', 'nerfhearder', 'nobody', 'nurd', 'nuts, numb', 'oddball', 'oger', 'oil dick', 'old fart', 'orang-uthan', 'original', 'outlaw', 'pack', 'pain in the ass', 'pavian', 'pencil dick', 'pervert', 'pig', 'piggy-wiggy', 'pirate', 'pornofreak', 'prick', 'prolet', 'queer', 'querulant', 'rat', 'rat-fink', 'reject', 'retard', 'riff-raff', 'ripper', 'roboter', 'rowdy', 'rufian', 'sack', 'sadist', 'saprophyt', 'satan', 'scarab', 'schfincter', 'shark', 'shit eater', 'shithead', 'simulant', 'skunk', 'skuz bag', 'slave', 'sleeze', 'sleeze bag', 'slimer', 'slimy bastard', 'small pricked', 'snail', 'snake', 'snob', 'snot', 'son of a bitch', 'square', 'stinker', 'stripper', 'stunk', 'swindler', 'swine', 'teletubby', 'thief', 'toilett cleaner', 'tussi', 'typ', 'unlike', 'vampir', 'vandale', 'varmit', 'wallflower', 'wanker', 'wanker, bloody', 'weeze bag', 'whore', 'wierdo', 'wino', 'witch', 'womanizer', 'woody allen', 'worm', 'xena', 'xenophebe', 'xenophobe', 'xxx watcher', 'yak', 'yeti', 'zit face']
  words = s["text"].lower()
  words_tweet=words.split(' ')
  time_at=s['created_at']
  tweet_hour=time_at.split(' ')[3].split(':')[0]
  for word in words_tweet:
    word_count +=1
    if word in badwords:
      bad_count += 1
  return (int(tweet_hour),bad_count)
badWordInfo = twitter.map(lambda tweet: hour_bad(json.loads(tweet)))
totalbadword=badWordInfo.reduceByKey(lambda x,y: x+y).collect()
totalWordInfo = twitter.map(lambda tweet: hour_good(json.loads(tweet)))
totalWord=totalWordInfo.reduceByKey(lambda x,y: x+y).collect()
totalbadword=dict(totalbadword)
totalWord=dict(totalWord)

result=set()
for key in totalbadword.keys():
  proportion=totalbadword[key]/totalWord[key]
  result.add((key,proportion))

sc.stop()

with open("q10",'w') as f:
    for element in result:
      f.write(str(element[0]) + '\t' + str(element[1]) + '\n')
