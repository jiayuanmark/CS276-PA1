#!/bin/env python
from collections import deque
import os, glob, os.path
import sys
import re

if len(sys.argv) != 2:
  print >> sys.stderr, 'usage: python query.py index_dir' 
  os._exit(-1)

# pop out postings
def popLeftOrNone(p):
  if len(p) > 0:
    posting = p.popleft()
  else:
    posting = None
  return posting

def merge_posting (postings1, postings2):
  new_posting = []
  pp1 = popLeftOrNone(postings1);
  pp2 = popLeftOrNone(postings2);
  while  pp1 is not None and pp2 is not None:
    if pp1 == pp2:
      new_posting.append(pp1)
      pp1 = popLeftOrNone(postings1)
      pp2 = popLeftOrNone(postings2)
    elif pp1 < pp2:
      pp1 = popLeftOrNone(postings1)
    else:
      pp2 = popLeftOrNone(postings2)
  return deque(new_posting)

# file locate of all the index related files
index_dir = sys.argv[1]
index_f = open(index_dir+'/corpus.index', 'r')
word_dict_f = open(index_dir+'/word.dict', 'r')
doc_dict_f = open(index_dir+'/doc.dict', 'r')
posting_dict_f = open(index_dir+'/posting.dict', 'r')

word_dict = {}
doc_id_dict = {}
file_pos_dict = {}
doc_freq_dict = {}

print >> sys.stderr, 'loading word dict'
for line in word_dict_f.readlines():
  parts = line.split('\t')
  word_dict[parts[0]] = int(parts[1])
print >> sys.stderr, 'loading doc dict'
for line in doc_dict_f.readlines():
  parts = line.split('\t')
  doc_id_dict[int(parts[1])] = parts[0]
print >> sys.stderr, 'loading index'
for line in posting_dict_f.readlines():
  parts = line.split('\t')
  term_id = int(parts[0])
  file_pos = int(parts[1])
  doc_freq = int(parts[2])
  file_pos_dict[term_id] = file_pos
  doc_freq_dict[term_id] = doc_freq

def read_posting(term_id):
  # provide implementation for posting list lookup for a given term
  # a useful function to use is index_f.seek(file_pos), which does a disc seek to 
  # a position offset 'file_pos' from the beginning of the file
  global index_f, file_pos_dict
  index_f.seek(file_pos_dict[term_id])
  line = index_f.readline().strip()
  return deque([int(u) for u in line.split()[1:]])

# read query from stdin
while True:
  input = sys.stdin.readline()
  input = input.strip()
  if len(input) == 0: # end of file reached
    break
  input_parts = input.split()
  # you need to translate words into word_ids
  # don't forget to handle the case where query contains unseen words
  # next retrieve the postings list of each query term, and merge the posting lists
  # to produce the final result
  query = []
  flag = False
  for item in input_parts:
    if item not in word_dict:
      print "no results found"
      flag = True
      break
    else:
      query.append((word_dict[item], doc_freq_dict[word_dict[item]]))

  if flag:
    continue

  query.sort(key = lambda x:x[1], reverse=False)
  result = read_posting(query[0][0])
  for idx in range(1, len(query)):
    result = merge_posting(result, read_posting(query[idx][0]))

  if len(result) == 0:
    print "no results found"
  else:
    doc_name = []
    for i in result:
      doc_name.append(doc_id_dict[i])
    doc_name.sort()
    for item in doc_name:
      print item

  # posting = read_posting(word_id)

  # don't forget to convert doc_id back to doc_name, and sort in lexicographical order
  # before printing out to stdout
